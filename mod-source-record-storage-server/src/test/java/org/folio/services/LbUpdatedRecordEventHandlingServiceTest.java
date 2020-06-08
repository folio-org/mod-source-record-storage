package org.folio.services;

import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.folio.dao.LbRecordDao;
import org.folio.dao.LbRecordDaoImpl;
import org.folio.dao.util.LbSnapshotDaoUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.TestUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jooq.Tables;
import org.folio.rest.util.OkapiConnectionParams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LbUpdatedRecordEventHandlingServiceTest extends AbstractLBServiceTest {

  private static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  private static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";
  private static final String UPDATED_PARSED_RECORD_CONTENT = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  private static final String PUBSUB_PUBLISH_URL = "/pubsub/publish";

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private LbRecordDao recordDao;
  
  private LbRecordService recordService;
  
  private LbUpdateRecordEventHandlingService updateRecordEventHandler;

  private OkapiConnectionParams params;

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private static String recordId = UUID.randomUUID().toString();

  private Record record;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);
    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, "token");
    params = new OkapiConnectionParams(headers, vertx);
    recordDao = new LbRecordDaoImpl(postgresClientFactory);
    recordService = new LbRecordServiceImpl(recordDao);
    updateRecordEventHandler = new LbUpdateRecordEventHandlingService(recordService);
    Async async = context.async();
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);
    LbSnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    LbSnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldUpdateParsedRecord(TestContext context) {
    Async async = context.async();
    
    ParsedRecord parsedRecord = record.getParsedRecord();
    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(record.getId())
      .withParsedRecord(new ParsedRecord()
        .withContent(UPDATED_PARSED_RECORD_CONTENT))
      .withRecordType(ParsedRecordDto.RecordType.MARC);

    WireMock.stubFor(post(PUBSUB_PUBLISH_URL)
      .willReturn(WireMock.noContent()));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("PARSED_RECORD_DTO", Json.encode(parsedRecordDto));

    Future<Boolean> future = recordService.saveRecord(record, TENANT_ID)
      .compose(rec -> {
        try {
          return updateRecordEventHandler.handleEvent(ZIPArchiver.zip(Json.encode(payloadContext)), params);
        } catch (IOException e) {
          e.printStackTrace();
          return Future.failedFuture(e);
        }
      });

    future.onComplete(ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      }
      recordService.getRecordById(record.getId(), TENANT_ID).onComplete(getNew -> {
        if (getNew.failed()) {
          context.fail(getNew.cause());
        }
        context.assertTrue(getNew.result().isPresent());
        Record updatedRecord = getNew.result().get();

        context.assertEquals(State.ACTUAL, updatedRecord.getState());
        context.assertEquals(1, updatedRecord.getGeneration());
        context.assertNotEquals(parsedRecord.getId(), updatedRecord.getParsedRecord().getId());
        context.assertNotEquals(record.getSnapshotId(), updatedRecord.getSnapshotId());

        recordDao.getRecordByCondition(Tables.RECORDS_LB.ID.eq(UUID.fromString(record.getId())), TENANT_ID).onComplete(getOld -> {
          if (getOld.failed()) {
            context.fail(getOld.cause());
          }
          context.assertTrue(getOld.result().isPresent());
          Record existingRecord = getOld.result().get();

          context.assertEquals(State.OLD, existingRecord.getState());
          context.assertEquals(0, existingRecord.getGeneration());
          context.assertEquals(parsedRecord.getId(), existingRecord.getParsedRecord().getId());
          context.assertEquals((String) parsedRecord.getContent(), (String) existingRecord.getParsedRecord().getContent());
          context.assertEquals(record.getSnapshotId(), existingRecord.getSnapshotId());
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenNoDataInPayload(TestContext context) {
    Async async = context.async();
    HashMap<String, String> payloadContext = new HashMap<>();

    Future<Boolean> future = updateRecordEventHandler.handleEvent(Json.encode(payloadContext), params);

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
