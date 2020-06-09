package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.RecordDaoImpl;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestVerticleTest;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

@RunWith(VertxUnitRunner.class)
public class InstanceEventHandlingServiceImplTest extends AbstractRestVerticleTest {

  private static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  private static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";
  private static final String PARSED_CONTENT_WITH_999_FIELD = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  private static final String RECORDS_TABLE_NAME = "records";
  private static final String RAW_RECORDS_TABLE_NAME = "raw_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";
  private static final String TENANT_ID = "diku";

  private Vertx vertx = Vertx.vertx();
  @Spy
  private PostgresClientFactory pgClientFactory = new PostgresClientFactory(Vertx.vertx());
  @Spy
  @InjectMocks
  private RecordDaoImpl recordDao;
  @Spy
  @InjectMocks
  private EventHandlingService eventHandlingService = new InstanceEventHandlingServiceImpl();

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private Record record;

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
    this.record = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(MARC);
  }

  @Override
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
  public void shouldSetInstanceIdToRecord(TestContext context) {
    Async async = context.async();
    record.withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(record.getId())
      .withSnapshotId(UUID.randomUUID().toString())
      .withDeleted(false)
      .withState(Record.State.ACTUAL);

    Record recordForUpdate = JsonObject.mapFrom(record
    ).mapTo(Record.class)
      .withSnapshotId(UUID.randomUUID().toString())
      .withId(UUID.randomUUID().toString());

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = new JsonObject()
      .put("id", expectedInstanceId)
      .put(expectedHrId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    HashMap<String, String> payloadContextForUpdate = new HashMap<>();
    payloadContextForUpdate.put(INSTANCE.value(), instance.encode());
    payloadContextForUpdate.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(recordForUpdate));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    DataImportEventPayload dataImportEventPayloadForUpdate = new DataImportEventPayload()
      .withContext(payloadContextForUpdate);

    Future<Boolean> future = recordDao.saveRecord(record, TENANT_ID)
      .compose(rec -> {
        try {
          return eventHandlingService.handleEvent(ZIPArchiver.zip(Json.encode(dataImportEventPayload)), TENANT_ID);
        } catch (IOException e) {
          e.printStackTrace();
          return Future.failedFuture(e);
        }
      });

    future.setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      recordDao.getRecordById(record.getId(), TENANT_ID).setHandler(getAr -> {
        context.assertTrue(getAr.succeeded());
        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        context.assertNotNull(updatedRecord.getExternalIdsHolder());
        context.assertTrue(expectedInstanceId.equals(updatedRecord.getExternalIdsHolder().getInstanceId()));

        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

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

        Future<Boolean> future2 = recordDao.saveRecord(recordForUpdate, TENANT_ID)
          .compose(v -> {
            try {
              return eventHandlingService.handleEvent(ZIPArchiver.zip(Json.encode(dataImportEventPayloadForUpdate)), TENANT_ID);
            } catch (IOException e) {
              e.printStackTrace();
              return Future.failedFuture(e);
            }
          });

        future2.setHandler(result -> {
          context.assertTrue(result.succeeded());
          recordDao.getRecordById(record.getId(), TENANT_ID)
            .setHandler(recordAr -> {
              context.assertTrue(recordAr.succeeded());
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
    record.withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withId(UUID.randomUUID().toString())
        .withContent(PARSED_CONTENT_WITH_999_FIELD))
      .withMatchedId(record.getId());

    String expectedInstanceId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), new JsonObject().put("id", expectedInstanceId).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    Future<Boolean> future = recordDao.saveRecord(record, TENANT_ID)
      .compose(rec -> {
        try {
          return eventHandlingService.handleEvent(ZIPArchiver.zip(Json.encode(dataImportEventPayload)), TENANT_ID);
        } catch (IOException e) {
          e.printStackTrace();
          return Future.failedFuture(e);
        }
      });

    future.setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      recordDao.getRecordById(record.getId(), TENANT_ID).setHandler(getAr -> {
        context.assertTrue(getAr.succeeded());
        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        context.assertNotNull(updatedRecord.getExternalIdsHolder());
        context.assertTrue(expectedInstanceId.equals(updatedRecord.getExternalIdsHolder().getInstanceId()));

        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

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

    Future<Boolean> future = eventHandlingService.handleEvent(Json.encode(dataImportEventPayload), TENANT_ID);

    future.setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenParsedRecordHasNoFields(TestContext context) {
    Async async = context.async();
    record.withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withId(UUID.randomUUID().toString())
        .withContent("{\"leader\":\"01240cas a2200397\"}"));

    String expectedInstanceId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), new JsonObject().put("id", expectedInstanceId).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    Future<Boolean> future = recordDao.saveRecord(record, TENANT_ID)
      .compose(rec -> eventHandlingService.handleEvent(Json.encode(dataImportEventPayload), TENANT_ID));

    future.setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
