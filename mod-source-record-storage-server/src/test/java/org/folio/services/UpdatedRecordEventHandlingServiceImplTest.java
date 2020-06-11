package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.SnapshotDao;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestVerticleTest;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.util.OkapiConnectionParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC;
import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class UpdatedRecordEventHandlingServiceImplTest extends AbstractRestVerticleTest {

  private static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  private static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";
  private static final String UPDATED_PARSED_RECORD_CONTENT = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  private static final String RECORDS_TABLE_NAME = "records";
  private static final String RAW_RECORDS_TABLE_NAME = "raw_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";
  private static final String TENANT_ID = "diku";
  private static final String PUBSUB_PUBLISH_URL = "/pubsub/publish";

  private Vertx vertx = Vertx.vertx();
  @Spy
  private PostgresClientFactory pgClientFactory = new PostgresClientFactory(Vertx.vertx());
  @Mock
  private SnapshotDao snapshotDao;
  @Spy
  @InjectMocks
  private RecordDaoImpl recordDao;
  @Spy
  @InjectMocks
  private RecordServiceImpl recordService;
  @Spy
  @InjectMocks
  private UpdateRecordEventHandler updateRecordEventHandler = new UpdateRecordEventHandler();

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private Record record;
  private OkapiConnectionParams params;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord()
      .withId(UUID.randomUUID().toString())
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord()
      .withId(UUID.randomUUID().toString())
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, "token");
    params = new OkapiConnectionParams(headers, vertx);
    String id = UUID.randomUUID().toString();
    this.record = new Record()
      .withId(id)
      .withSnapshotId(UUID.randomUUID().toString())
      .withGeneration(0)
      .withMatchedId(id)
      .withRecordType(MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);
  }

  @Before
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(RECORDS_TABLE_NAME, new Criterion(), event ->
      pgClient.delete(RAW_RECORDS_TABLE_NAME, new Criterion(), event1 ->
        pgClient.delete(MARC_RECORDS_TABLE_NAME, new Criterion(), event2 -> {
          if (event2.failed()) {
            context.fail(event2.cause());
          }
          async.complete();
        })));
  }

  @Test
  public void shouldUpdateParsedRecord(TestContext context) {
    Async async = context.async();
    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(record.getId())
      .withParsedRecord(parsedRecord.withContent(UPDATED_PARSED_RECORD_CONTENT))
      .withRecordType(ParsedRecordDto.RecordType.MARC);

    WireMock.stubFor(post(PUBSUB_PUBLISH_URL)
      .willReturn(WireMock.noContent()));

    when(snapshotDao.getSnapshotById(record.getSnapshotId(), TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.of(new Snapshot()
        .withJobExecutionId(record.getSnapshotId())
        .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED))));

    when(snapshotDao.saveSnapshot(any(Snapshot.class), anyString()))
      .thenReturn(Future.succeededFuture(new Snapshot().withJobExecutionId(UUID.randomUUID().toString())));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("PARSED_RECORD_DTO", Json.encode(parsedRecordDto));

    Future<Boolean> future = recordService.saveRecord(record, TENANT_ID)
      .compose(rec -> {
        try {
          return updateRecordEventHandler.handle(ZIPArchiver.zip(Json.encode(payloadContext)), params);
        } catch (IOException e) {
          e.printStackTrace();
          return Future.failedFuture(e);
        }
      });

    future.setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      recordService.getRecordById(record.getId(), TENANT_ID).setHandler(getAr -> {
        context.assertTrue(getAr.succeeded());
        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        context.assertEquals(ACTUAL, updatedRecord.getState());
        context.assertEquals(1, updatedRecord.getGeneration());
        context.assertNotEquals(parsedRecord.getContent(), UPDATED_PARSED_RECORD_CONTENT);
        context.assertNotEquals(parsedRecord.getId(), updatedRecord.getParsedRecord().getId());
        context.assertNotEquals(record.getSnapshotId(), updatedRecord.getSnapshotId());
        async.complete();
      });
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenNoDataInPayload(TestContext context) {
    Async async = context.async();
    HashMap<String, String> payloadContext = new HashMap<>();

    Future<Boolean> future = updateRecordEventHandler.handle(Json.encode(payloadContext), params);

    future.setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
