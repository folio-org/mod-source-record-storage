package org.folio.services;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.services.handlers.InstancePostProcessingEventHandler;
import org.folio.services.util.AdditionalFieldsUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

// TODO: fix @Ignore tests in scope of MODSOURCE-235
@RunWith(VertxUnitRunner.class)
public class InstancePostProcessingEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT_WITH_999_FIELD = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";
  private static final String PARSED_CONTENT_WITHOUT_001_FIELD = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  private RecordDao recordDao;
  private InstancePostProcessingEventHandler instancePostProcessingEventHandler;

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private static String recordId = UUID.randomUUID().toString();

  private String snapshotId1 = UUID.randomUUID().toString();
  private String snapshotId2 = UUID.randomUUID().toString();

  private Record record;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);

    recordDao = new RecordDaoImpl(postgresClientFactory);
    instancePostProcessingEventHandler = new InstancePostProcessingEventHandler(recordDao, vertx, kafkaConfig);
    Async async = context.async();

    Snapshot snapshot1 = new Snapshot()
      .withJobExecutionId(snapshotId1)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    Snapshot snapshot2 = new Snapshot()
      .withJobExecutionId(snapshotId2)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    List<Snapshot> snapshots = new ArrayList<>();
    snapshots.add(snapshot1);
    snapshots.add(snapshot2);

    this.record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)

      .withExternalIdsHolder(null);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshots).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  @Ignore
  public void shouldSetInstanceIdToRecord(TestContext context) {
    Async async = context.async();

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = new JsonObject()
      .put("id", expectedInstanceId)
      .put("hrid", expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(record -> instancePostProcessingEventHandler.handle(dataImportEventPayload)
        .thenApply(payload -> future.complete(payload))
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, e) -> {
      if (e != null) {
        context.fail(e);
      }
      recordDao.getRecordByMatchedId(record.getMatchedId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }

        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        context.assertNotNull(updatedRecord.getExternalIdsHolder());
        context.assertEquals(expectedInstanceId, updatedRecord.getExternalIdsHolder().getInstanceId());

        context.assertNotNull(updatedRecord.getParsedRecord());
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

        String recordForUdateId = UUID.randomUUID().toString();
        Record recordForUpdate = JsonObject.mapFrom(record).mapTo(Record.class)
          .withId(recordForUdateId)
          .withSnapshotId(snapshotId2)
          .withRawRecord(record.getRawRecord().withId(recordForUdateId))
          .withParsedRecord(record.getParsedRecord().withId(recordForUdateId))
          .withGeneration(1);

        HashMap<String, String> payloadContextForUpdate = new HashMap<>();
        payloadContextForUpdate.put(INSTANCE.value(), instance.encode());
        payloadContextForUpdate.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(recordForUpdate));

        DataImportEventPayload dataImportEventPayloadForUpdate = new DataImportEventPayload()
          .withContext(payloadContextForUpdate)
          .withTenant(TENANT_ID);

        CompletableFuture<DataImportEventPayload> future2 = new CompletableFuture<>();
        recordDao.saveRecord(recordForUpdate, TENANT_ID)
          .onFailure(future2::completeExceptionally)
          .onSuccess(record -> instancePostProcessingEventHandler.handle(dataImportEventPayloadForUpdate)
            .thenApply(future2::complete)
            .exceptionally(future2::completeExceptionally));

        future2.whenComplete((payload2, ex) -> {
          if (ex != null) {
            context.fail(ex);
          }
          recordDao.getRecordByMatchedId(record.getMatchedId(), TENANT_ID).onComplete(recordAr -> {
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
  @Ignore
  public void shouldSaveRecordWhenRecordDoesntExist(TestContext context) throws IOException {
    Async async = context.async();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());

    Record defaultRecord = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = new JsonObject()
      .put("id", expectedInstanceId)
      .put("hrid", expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    instancePostProcessingEventHandler.handle(dataImportEventPayload)
      .thenApply(future::complete)
      .exceptionally(future::completeExceptionally);

    future.whenComplete((payload, e) -> {
      if (e != null) {
        context.fail(e);
      }
      recordDao.getRecordByMatchedId(defaultRecord.getMatchedId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }

        context.assertTrue(getAr.result().isPresent());
        Record savedRecord = getAr.result().get();

        context.assertNotNull(savedRecord.getExternalIdsHolder());
        context.assertEquals(expectedInstanceId, savedRecord.getExternalIdsHolder().getInstanceId());

        context.assertNotNull(savedRecord.getParsedRecord());
        context.assertNotNull(savedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(savedRecord.getParsedRecord().getContent());

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
  @Ignore
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
      .withContext(payloadContext)
      .withTenant(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(rec -> instancePostProcessingEventHandler.handle(dataImportEventPayload)
        .thenApply(payload -> future.complete(payload))
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        context.fail(throwable);
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
  @Ignore
  public void shouldUpdateField005WhenThisFiledIsNotProtected(TestContext context) throws IOException {
    Async async = context.async();

    String expectedDate = AdditionalFieldsUtil.dateTime005Formatter
      .format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());

    Record defaultRecord = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = new JsonObject()
      .put("id", expectedInstanceId)
      .put("hrid", expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    instancePostProcessingEventHandler.handle(dataImportEventPayload)
      .thenApply(future::complete)
      .exceptionally(future::completeExceptionally);

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        context.fail(throwable);
      }
      recordDao.getRecordByMatchedId(defaultRecord.getMatchedId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }

        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        String actualDate = AdditionalFieldsUtil.getValueFromControlledField(updatedRecord, TAG_005);
        Assert.assertEquals(expectedDate.substring(0, 10),
          actualDate.substring(0, 10));

        async.complete();
      });
    });
  }

  @Test
  @Ignore
  public void shouldUpdateField005WhenThisFiledIsProtected(TestContext context) throws IOException {
    Async async = context.async();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());

    Record defaultRecord = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(Arrays.asList(new MarcFieldProtectionSetting()
        .withField(TAG_005)
        .withData("*")));

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = new JsonObject()
      .put("id", expectedInstanceId)
      .put("hrid", expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));
    payloadContext.put("MAPPING_PARAMS", Json.encodePrettily(mappingParameters));

    String expectedDate = AdditionalFieldsUtil.getValueFromControlledField(record, TAG_005);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    instancePostProcessingEventHandler.handle(dataImportEventPayload)
      .thenApply(future::complete)
      .exceptionally(future::completeExceptionally);

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        context.fail(throwable);
      }
      recordDao.getRecordByMatchedId(defaultRecord.getMatchedId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }

        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        String actualDate = AdditionalFieldsUtil.getValueFromControlledField(updatedRecord, TAG_005);
        Assert.assertEquals(expectedDate, actualDate);

        async.complete();
      });
    });
  }

  @Test
  @Ignore
  public void shouldSetInstanceHridToParsedRecordWhenContentHasNotField001(TestContext context) {
    Async async = context.async();

    record.withParsedRecord(new ParsedRecord()
      .withId(recordId)
      .withContent(PARSED_CONTENT_WITHOUT_001_FIELD));

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedInstanceHrid = UUID.randomUUID().toString();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), new JsonObject().put("id", expectedInstanceId).put("hrid", expectedInstanceHrid).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(rec -> instancePostProcessingEventHandler.handle(dataImportEventPayload)
        .thenApply(payload -> future.complete(payload))
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        context.fail(throwable);
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
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualInstanceHrid = null;
        for (int i = 0; i < fields.size(); i++) {
          JsonObject field = fields.getJsonObject(i);
          if (field.containsKey("001")) {
            actualInstanceHrid = field.getString("001");
          }
        }
        context.assertEquals(expectedInstanceHrid, actualInstanceHrid);
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

    CompletableFuture<DataImportEventPayload> future = instancePostProcessingEventHandler.handle(dataImportEventPayload);

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
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

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(record -> instancePostProcessingEventHandler.handle(dataImportEventPayload)
        .thenApply(future::complete)
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, throwable) -> {
      context.assertNotNull(throwable);
      async.complete();
    });
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForProfile() {
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(mappingProfile.getId())
      .withContentType(MAPPING_PROFILE)
      .withContent(mappingProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType("DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING")
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = instancePostProcessingEventHandler.isEligible(dataImportEventPayload);

    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForProfile() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = instancePostProcessingEventHandler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }

}
