package org.folio.services;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.folio.TestMocks;
import org.folio.dao.LbRecordDao;
import org.folio.dao.LbRecordDaoImpl;
import org.folio.dao.util.LbSnapshotDaoUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.TestUtil;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LbInstanceEventHandlingServiceTest extends AbstractLBServiceTest {

  private static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  private static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";
  private static final String PARSED_CONTENT_WITH_999_FIELD = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));
  
  private LbRecordDao recordDao;

  private LbInstanceEventHandlingService eventHandlingService;

  private OkapiConnectionParams params;

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private Record record;

  private static String recordId = UUID.randomUUID().toString();

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
    eventHandlingService = new LbInstanceEventHandlingService(recordDao);
    Async async = context.async();
    this.record = TestMocks.getRecord(0)
      .withId(recordId)
      .withMatchedId(recordId)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(null);
    TestMocks.getSnapshot(0)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    TestMocks.getSnapshot(1)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    LbSnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots()).onComplete(save -> {
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
  public void shouldSetInstanceIdToRecord(TestContext context) {
    Async async = context.async();

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = new JsonObject()
      .put("id", expectedInstanceId)
      .put(expectedHrId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    Future<Boolean> future = recordDao.saveRecord(record, TENANT_ID)
      .compose(rec -> {
        try {
          return eventHandlingService.handleEvent(ZIPArchiver.zip(Json.encode(dataImportEventPayload)), params);
        } catch (IOException e) {
          e.printStackTrace();
          return Future.failedFuture(e);
        }
      });

    future.onComplete(ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      }
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }

        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        context.assertNotNull(updatedRecord.getExternalIdsHolder());
        context.assertEquals(expectedInstanceId, updatedRecord.getExternalIdsHolder().getInstanceId());

        context.assertNotNull(updatedRecord.getParsedRecord());
        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = new JsonObject((String) updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualInstanceId = null;
        for (int i = 0; i < fields.size(); i++) {
          JsonObject field = fields.getJsonObject(i);
          if (field.containsKey(TAG_999)) {
            JsonArray subfields = field.getJsonObject(TAG_999).getJsonArray("subfields");
            for (int j = 0; j < subfields.size(); j++) {
              JsonObject subfield = subfields.getJsonObject(j);
              if (subfield.containsKey("i")) {
                actualInstanceId = subfield.getString("i");
              }
            }
          }
        }
        context.assertEquals(expectedInstanceId, actualInstanceId);

        String recordForUdateId = UUID.randomUUID().toString();
        Record recordForUpdate = JsonObject.mapFrom(record).mapTo(Record.class)
          .withId(recordForUdateId)
          .withSnapshotId(TestMocks.getSnapshot(1).getJobExecutionId())
          .withRawRecord(record.getRawRecord().withId(recordForUdateId))
          .withParsedRecord(record.getParsedRecord().withId(recordForUdateId))
          .withGeneration(1);

        HashMap<String, String> payloadContextForUpdate = new HashMap<>();
        payloadContextForUpdate.put(INSTANCE.value(), instance.encode());
        payloadContextForUpdate.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(recordForUpdate));  

        DataImportEventPayload dataImportEventPayloadForUpdate = new DataImportEventPayload()
          .withContext(payloadContextForUpdate);

        Future<Boolean> future2 = recordDao.saveRecord(recordForUpdate, TENANT_ID)
          .compose(v -> {
            try {
              return eventHandlingService.handleEvent(ZIPArchiver.zip(Json.encode(dataImportEventPayloadForUpdate)), params);
            } catch (IOException e) {
              e.printStackTrace();
              return Future.failedFuture(e);
            }
          });

        future2.onComplete(result -> {
          if (result.failed()) {
            context.fail(result.cause());
          }
          recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(recordAr -> {
            if (recordAr.failed()) {
              context.fail(recordAr.cause());
            }
            context.assertTrue(recordAr.result().isPresent());
            Record rec = recordAr.result().get();
            context.assertTrue(rec.getState().equals(Record.State.ACTUAL));
            context.assertNotNull(rec.getExternalIdsHolder());
            context.assertTrue(expectedInstanceId.equals(rec.getExternalIdsHolder().getInstanceId()));
            context.assertNotEquals(rec.getId(), record.getId());
            async.complete();
          });
        });
      });
    });
  }

  @Test
  public void shouldSetInstanceIdToParsedRecordWhenContentHasField999(TestContext context) {
    Async async = context.async();
    record.withParsedRecord(new ParsedRecord()
        .withId(recordId)
        .withContent(PARSED_CONTENT_WITH_999_FIELD));

    String expectedInstanceId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), new JsonObject().put("id", expectedInstanceId).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    Future<Boolean> future = recordDao.saveRecord(record, TENANT_ID)
      .compose(rec -> {
        try {
          return eventHandlingService.handleEvent(ZIPArchiver.zip(Json.encode(dataImportEventPayload)), params);
        } catch (IOException e) {
          e.printStackTrace();
          return Future.failedFuture(e);
        }
      });

    future.onComplete(ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      }
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }
        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        context.assertNotNull(updatedRecord.getExternalIdsHolder());
        context.assertTrue(expectedInstanceId.equals(updatedRecord.getExternalIdsHolder().getInstanceId()));

        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = new JsonObject((String) updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualInstanceId = null;
        for (int i = 0; i < fields.size(); i++) {
          JsonObject field = fields.getJsonObject(i);
          if (field.containsKey(TAG_999)) {
            JsonArray subfields = field.getJsonObject(TAG_999).getJsonArray("subfields");
            for (int j = 0; j < subfields.size(); j++) {
              JsonObject subfield = subfields.getJsonObject(j);
              if (subfield.containsKey("i")) {
                actualInstanceId = subfield.getString("i");
              }
            }
          }
        }
        context.assertEquals(expectedInstanceId, actualInstanceId);
        async.complete();
      });
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenInstanceOrRecordDoesNotExist(TestContext context) {
    Async async = context.async();
    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    Future<Boolean> future = eventHandlingService.handleEvent(Json.encode(dataImportEventPayload), params);

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenParsedRecordHasNoFields(TestContext context) {
    Async async = context.async();
    record.withParsedRecord(new ParsedRecord()
      .withId(record.getId())
      .withContent("{\"leader\":\"01240cas a2200397\"}"));

    String expectedInstanceId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), new JsonObject().put("id", expectedInstanceId).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    Future<Boolean> future = recordDao.saveRecord(record, TENANT_ID)
      .compose(rec -> eventHandlingService.handleEvent(Json.encode(dataImportEventPayload), params));

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
