package org.folio.services.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Map;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;
import org.folio.services.SnapshotService;
import org.folio.services.SnapshotServiceImpl;
import org.folio.services.exceptions.DuplicateRecordException;
import org.folio.services.util.AdditionalFieldsUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ORDER_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.services.handlers.InstancePostProcessingEventHandler.POST_PROCESSING_RESULT_EVENT;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(VertxUnitRunner.class)
public class InstancePostProcessingEventHandlerTest extends AbstractPostProcessingEventHandlerTest {

  public static final String CENTRAL_TENANT_INSTANCE_UPDATED_FLAG = "CENTRAL_TENANT_INSTANCE_UPDATED";
  public static final String CENTRAL_TENANT_ID = "CENTRAL_TENANT_ID";

  @Mock
  private RecordServiceImpl mockedRecordService;

  @Mock
  private SnapshotServiceImpl mockedSnapshotService;
  @Mock
  private RecordCollection recordCollection;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Override
  protected Record.RecordType getMarcType() {
    return MARC_BIB;
  }

  @Override
  protected AbstractPostProcessingEventHandler createHandler(RecordService recordService, SnapshotService snapshotService, KafkaConfig kafkaConfig) {
    return new InstancePostProcessingEventHandler(recordService, snapshotService, kafkaConfig, mappingParametersCache, vertx);
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
    payloadContext.put("recordId", record.getId());

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
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
        recordDao.saveRecord(recordForUpdate, okapiHeaders)
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
  public void shouldProceedIfConsortiumTrackExists(TestContext context) {
    MockitoAnnotations.openMocks(this);

    Async async = context.async();

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(record))).when(mockedRecordService).getRecordById(anyString(), anyString());

    doAnswer(invocationOnMock -> Future.succeededFuture(new Snapshot())).when(mockedSnapshotService).copySnapshotToOtherTenant(anyString(), anyString(), anyString());

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(record))).when(mockedRecordService).getRecordById(anyString(), anyString());

    doAnswer(invocationOnMock -> Future.succeededFuture(record.getParsedRecord())).when(mockedRecordService).updateParsedRecord(any(), anyString());

    doAnswer(invocationOnMock -> Future.succeededFuture(recordCollection)).when(mockedRecordService).getRecords(any(), any(), any(), anyInt(), anyInt(), anyString());

    doAnswer(invocationOnMock -> List.of(record)).when(recordCollection).getRecords();

    doAnswer(invocationOnMock -> Future.succeededFuture(record)).when(mockedRecordService).updateRecord(any(), any());

    InstancePostProcessingEventHandler handler = new InstancePostProcessingEventHandler(mockedRecordService, mockedSnapshotService, kafkaConfig, mappingParametersCache, vertx);

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();
    String expectedCentralTenantId = "centralTenantId";

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("recordId", record.getId());
    payloadContext.put(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG, "true");
    payloadContext.put(CENTRAL_TENANT_ID, expectedCentralTenantId);

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
      verify(mockedRecordService, times(1)).updateParsedRecord(any(), anyString());
      context.assertNull(payload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG));
      context.assertEquals(expectedCentralTenantId, payload.getContext().get(CENTRAL_TENANT_ID));
      async.complete();
    });
  }

  @Test
  public void shouldSaveRecordWhenRecordDoesntExist(TestContext context) throws IOException {
    Async async = context.async();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record defaultRecord = new Record()
      .withId(recordId)
      .withSnapshotId(snapshotId1)
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

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

    future.whenComplete((payload, e) -> {
      if (e != null) {
        context.fail(e);
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
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
  public void shouldReturnExceptionForDuplicateRecord(TestContext context) throws IOException {
    Async async = context.async();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record defaultRecord = new Record()
      .withId(recordId)
      .withSnapshotId(snapshotId1)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String duplicateRecordId = UUID.randomUUID().toString();
    RawRecord rawRecordDuplicate = new RawRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH));
    ParsedRecord parsedRecordDuplicate = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record duplicateRecord = new Record()
      .withId(duplicateRecordId)
      .withSnapshotId(snapshotId1)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecordDuplicate)
      .withParsedRecord(parsedRecordDuplicate);

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContextOriginalRecord = new HashMap<>();
    payloadContextOriginalRecord.put(INSTANCE.value(), instance.encode());
    payloadContextOriginalRecord.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));

    HashMap<String, String> payloadContextDuplicateRecord = new HashMap<>();
    payloadContextDuplicateRecord.put(INSTANCE.value(), instance.encode());
    payloadContextDuplicateRecord.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(duplicateRecord));

    DataImportEventPayload dataImportEventPayloadOriginalRecord =
      createDataImportEventPayload(payloadContextOriginalRecord, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    DataImportEventPayload dataImportEventPayloadDuplicateRecord =
      createDataImportEventPayload(payloadContextDuplicateRecord, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future1 = handler.handle(dataImportEventPayloadOriginalRecord);

    future1.whenComplete((payload, e) -> {
      if (e != null) {
        context.fail(e);
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
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
      });
      CompletableFuture<DataImportEventPayload> future2 = handler.handle(dataImportEventPayloadDuplicateRecord);
      future2.whenComplete((eventPayload, throwable) -> {
        context.assertNotNull(throwable);
        context.assertEquals(throwable.getClass(), DuplicateRecordException.class);
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveIncomingRecordAndMarkExistingAsOldWhenIncomingRecordHasSameMatchedId(TestContext context) throws IOException {
    Async async = context.async();
    Record existingRecord = TestMocks.getRecord(0);
    existingRecord.setSnapshotId(snapshotId1);

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record incomingRecord = new Record()
      .withRawRecord(rawRecord)
      .withId(recordId)
      .withSnapshotId(snapshotId2)
      .withRecordType(MARC_BIB)
      .withParsedRecord(parsedRecord);

    JsonObject instanceJson = createExternalEntity(UUID.randomUUID().toString(), "in001");
    existingRecord.getExternalIdsHolder().setInstanceId(instanceJson.getString("id"));
    existingRecord.getExternalIdsHolder().setInstanceHrid(instanceJson.getString("hrid"));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instanceJson.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING);

    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    Future<DataImportEventPayload> future = recordDao.saveRecord(existingRecord, okapiHeaders)
      .compose(v -> Future.fromCompletionStage(handler.handle(dataImportEventPayload)));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());

      recordDao.getRecordById(existingRecord.getId(), TENANT_ID).onComplete(recordAr -> {
        context.assertTrue(recordAr.succeeded());
        context.assertTrue(recordAr.result().isPresent());
        Record existingRec = recordAr.result().get();
        context.assertEquals(Record.State.OLD, existingRec.getState());

        Record savedIncomingRecord = Json.decodeValue(ar.result().getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
        context.assertEquals(Record.State.ACTUAL, savedIncomingRecord.getState());
        context.assertNotNull(savedIncomingRecord.getGeneration());
        context.assertTrue(existingRec.getGeneration() < savedIncomingRecord.getGeneration());

        async.complete();
      });
    });
  }

  @Test
  public void checkGeneration035FiledAfterUpdateMarcBib(TestContext context) throws IOException {
    Async async = context.async();
    Record existingRecord = TestMocks.getRecord(0);
    existingRecord.setSnapshotId(snapshotId1);

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH_035_CHECK));

    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(
        TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH_035_CHECK), JsonObject.class).encode());

    Record incomingRecord = new Record()
      .withRawRecord(rawRecord)
      .withId(recordId)
      .withSnapshotId(snapshotId2)
      .withRecordType(MARC_BIB)
      .withParsedRecord(parsedRecord);

    JsonObject instanceJson = createExternalEntity(UUID.randomUUID().toString(), "in00000000040");
    existingRecord.getExternalIdsHolder().setInstanceId(instanceJson.getString("id"));
    existingRecord.getExternalIdsHolder().setInstanceHrid(instanceJson.getString("hrid"));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instanceJson.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING);

    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    Future<DataImportEventPayload> future = recordDao.saveRecord(existingRecord, okapiHeaders)
      .compose(v -> Future.fromCompletionStage(handler.handle(dataImportEventPayload)));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());

      recordDao.getRecordById(existingRecord.getId(), TENANT_ID).onComplete(recordAr -> {
        context.assertTrue(recordAr.succeeded());
        context.assertTrue(recordAr.result().isPresent());

        Record existingRec = recordAr.result().get();
        context.assertEquals(Record.State.OLD, existingRec.getState());

        Record savedIncomingRecord = Json.decodeValue(ar.result().getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
        context.assertEquals(Record.State.ACTUAL, savedIncomingRecord.getState());
        context.assertNotNull(savedIncomingRecord.getGeneration());
        context.assertTrue(existingRec.getGeneration() < savedIncomingRecord.getGeneration());
        context.assertFalse(((String) savedIncomingRecord.getParsedRecord().getContent()).contains("(LTSA)in00000000040"));

        async.complete();
      });
    });
  }

  @Test
  public void shouldSetInstanceIdToParsedRecordWhenContentHasField999(TestContext context) {
    Async async = context.async();
    String expectedInstanceId = UUID.randomUUID().toString();
    var expectedHrid = "in0002";

    record.withParsedRecord(new ParsedRecord()
        .withId(recordId)
        .withContent(PARSED_CONTENT_WITH_999_FIELD))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceHrid("in0001").withInstanceId(expectedInstanceId));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), new JsonObject().put("id", expectedInstanceId).put("hrid", expectedHrid).encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
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
        var actualInventoryHrid = getInventoryHrid(fields);
        context.assertEquals(expectedHrid, actualInventoryHrid);
        async.complete();
      });
    });
  }

  @Test
  public void shouldUpdateField005WhenThisFiledIsNotProtected(TestContext context) throws IOException {
    Async async = context.async();

    String expectedDate = get005FieldExpectedDate();

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record defaultRecord = new Record()
      .withId(recordId)
      .withSnapshotId(snapshotId1)
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

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        context.fail(throwable);
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          context.fail(getAr.cause());
        }

        context.assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        validate005Field(context, expectedDate, updatedRecord);

        async.complete();
      });
    });
  }

  @Test
  public void shouldUpdateField005WhenThisFiledIsProtected(TestContext context) throws IOException {
    Async async = context.async();

    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(List.of(new MarcFieldProtectionSetting()
        .withField(TAG_005)
        .withData("*")));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA__URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(mappingParameters))))));

    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record defaultRecord = new Record()
      .withId(recordId)
      .withSnapshotId(snapshotId1)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(defaultRecord));

    String expectedDate = AdditionalFieldsUtil.getValueFromControlledField(record, TAG_005);

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        context.fail(throwable);
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
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
    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
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
    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
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

  @Test
  public void shouldFillEventPayloadWithPostProcessingFlagIfOrderEventExists(TestContext context) {
    Async async = context.async();

    String expectedInstanceId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject instance = createExternalEntity(expectedInstanceId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(INSTANCE.value(), instance.encode());
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("recordId", record.getId());

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_ORDER_CREATED_READY_FOR_POST_PROCESSING);
    dataImportEventPayload.getContext().put(POST_PROCESSING_RESULT_EVENT, DI_ORDER_CREATED_READY_FOR_POST_PROCESSING.value());

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var okapiHeaders = Map.of(TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
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
        context.assertNull(payload.getContext().get(POST_PROCESSING_RESULT_EVENT));
        context.assertEquals(Boolean.valueOf(payload.getContext().get("POST_PROCESSING")), true);
        context.assertEquals(payload.getEventType(), DI_ORDER_CREATED_READY_FOR_POST_PROCESSING.value());

        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualInstanceId = getInventoryId(fields);
        context.assertEquals(expectedInstanceId, actualInstanceId);
        async.complete();
      });
    });
  }
}
