package org.folio.services.handlers;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.AdditionalFieldsUtil;

@RunWith(VertxUnitRunner.class)
public class InstancePostProcessingEventHandlerTest extends AbstractPostProcessingEventHandlerTest {

  @Override
  protected Record.RecordType getMarcType() {
    return MARC_BIB;
  }

  @Override
  protected AbstractPostProcessingEventHandler createHandler(RecordDao recordDao, KafkaConfig kafkaConfig) {
    return new InstancePostProcessingEventHandler(recordDao, kafkaConfig);
  }

  @Test
  public void shouldSetInstanceIdToRecord(TestContext context) {
    Async async = context.async();

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("correlationId", "1");

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(record -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
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

        String actualInstanceId = getInventoryId(fields);
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

        DataImportEventPayload dataImportEventPayloadForUpdate =
          createDataImportEventPayload(payloadContextForUpdate, DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING);

        CompletableFuture<DataImportEventPayload> future2 = new CompletableFuture<>();
        recordDao.saveRecord(recordForUpdate, TENANT_ID)
          .onFailure(future2::completeExceptionally)
          .onSuccess(record -> handler.handle(dataImportEventPayloadForUpdate)
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
  public void shouldSaveRecordWhenRecordDoesntExist(TestContext context) throws IOException {
    Async async = context.async();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
          .encode());

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

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    handler.handle(dataImportEventPayload)
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

        String actualInstanceId = getInventoryId(fields);
        context.assertEquals(expectedInstanceId, actualInstanceId);
        async.complete();
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

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(rec -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
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

        String actualInstanceId = getInventoryId(fields);
        context.assertEquals(expectedInstanceId, actualInstanceId);
        async.complete();
      });
    });
  }

  @Test
  public void shouldUpdateField005WhenThisFiledIsNotProtected(TestContext context) throws IOException {
    Async async = context.async();

    String expectedDate = AdditionalFieldsUtil.dateTime005Formatter
      .format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
          .encode());

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

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    handler.handle(dataImportEventPayload)
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
  public void shouldUpdateField005WhenThisFiledIsProtected(TestContext context) throws IOException {
    Async async = context.async();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
          .encode());

    Record defaultRecord = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(List.of(new MarcFieldProtectionSetting()
        .withField(TAG_005)
        .withData("*")));

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));
    payloadContext.put("MAPPING_PARAMS", Json.encodePrettily(mappingParameters));

    String expectedDate = AdditionalFieldsUtil.getValueFromControlledField(record, TAG_005);

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    handler.handle(dataImportEventPayload)
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
  public void shouldSetInstanceHridToParsedRecordWhenContentHasNotField001(TestContext context) {
    Async async = context.async();

    record.withParsedRecord(new ParsedRecord()
      .withId(recordId)
      .withContent(PARSED_CONTENT_WITHOUT_001_FIELD));

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedInstanceHrid = UUID.randomUUID().toString();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(),
      createExternalEntity(expectedInstanceId, expectedInstanceHrid).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(rec -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
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

        String actualInstanceHrid = getInventoryHrid(fields);
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
      .withEventType(DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext);

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

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
      .withEventType(DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    recordDao.saveRecord(record, TENANT_ID)
      .onFailure(future::completeExceptionally)
      .onSuccess(record -> handler.handle(dataImportEventPayload)
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
      .withEventType(DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenRecordTypeIsNotInstance() {
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(MARC_BIBLIOGRAPHIC);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(mappingProfile.getId())
      .withContentType(MAPPING_PROFILE)
      .withContent(mappingProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
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

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenCurrentNodeIsNull() {
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
      .withProfileSnapshot(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }

}
