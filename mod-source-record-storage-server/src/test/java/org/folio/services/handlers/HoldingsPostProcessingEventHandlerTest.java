package org.folio.services.handlers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
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
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.junit.Assert;
import org.junit.Rule;
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
public class HoldingsPostProcessingEventHandlerTest extends AbstractPostProcessingEventHandlerTest {

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Override
  protected Record.RecordType getMarcType() {
    return MARC_HOLDING;
  }

  @Override
  protected AbstractPostProcessingEventHandler createHandler(RecordDao recordDao, KafkaConfig kafkaConfig) {
    return new HoldingsPostProcessingEventHandler(recordDao, kafkaConfig, mappingParametersCache, vertx);
  }

  @Test
  public void shouldSetHoldingsIdToRecord(TestContext context) {
    Async async = context.async();

    String expectedHoldingsId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject holdings = createExternalEntity(expectedHoldingsId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), holdings.encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

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
        context.assertEquals(expectedHoldingsId, updatedRecord.getExternalIdsHolder().getHoldingsId());

        context.assertNotNull(updatedRecord.getParsedRecord());
        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualHoldingsId = getInventoryId(fields);
        context.assertEquals(expectedHoldingsId, actualHoldingsId);
        async.complete();
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
      .withRecordType(MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String expectedHoldingsId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject holdings = createExternalEntity(expectedHoldingsId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), holdings.encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(defaultRecord));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

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
        context.assertEquals(expectedHoldingsId, savedRecord.getExternalIdsHolder().getHoldingsId());

        context.assertNotNull(savedRecord.getParsedRecord());
        context.assertNotNull(savedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(savedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualHoldingsId = getInventoryId(fields);
        context.assertEquals(expectedHoldingsId, actualHoldingsId);
        async.complete();
      });
    });
  }

  @Test
  public void shouldSetHoldingsIdToParsedRecordWhenContentHasField999(TestContext context) {
    Async async = context.async();

    record.withParsedRecord(new ParsedRecord()
      .withId(recordId)
      .withContent(PARSED_CONTENT_WITH_999_FIELD));

    String expectedHoldingsId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), new JsonObject().put("id", expectedHoldingsId).encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

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
        context.assertTrue(expectedHoldingsId.equals(updatedRecord.getExternalIdsHolder().getHoldingsId()));

        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualHoldingsId = getInventoryId(fields);
        context.assertEquals(expectedHoldingsId, actualHoldingsId);
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
      .withRecordType(MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String expectedHoldingsId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject holdings = createExternalEntity(expectedHoldingsId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), holdings.encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(defaultRecord));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

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
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
          .encode());

    Record defaultRecord = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    String expectedHoldingsId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject holdings = createExternalEntity
      (expectedHoldingsId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), holdings.encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(defaultRecord));

    String expectedDate = AdditionalFieldsUtil.getValueFromControlledField(record, TAG_005);

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

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
  public void shouldSetHoldingsHridToParsedRecordWhenContentHasNotField001(TestContext context) {
    Async async = context.async();

    record.withParsedRecord(new ParsedRecord()
      .withId(recordId)
      .withContent(PARSED_CONTENT_WITHOUT_001_FIELD));

    String expectedHoldingsId = UUID.randomUUID().toString();
    String expectedHoldingsHrid = UUID.randomUUID().toString();

    var holdings = createExternalEntity(expectedHoldingsId, expectedHoldingsHrid);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), holdings.encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

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
        context.assertTrue(expectedHoldingsId.equals(updatedRecord.getExternalIdsHolder().getHoldingsId()));

        context.assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        context.assertTrue(!fields.isEmpty());

        String actualHoldingsHrid = getInventoryHrid(fields);
        context.assertEquals(expectedHoldingsHrid, actualHoldingsHrid);
        async.complete();
      });
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenHoldingsOrRecordDoesNotExist(TestContext context) {
    Async async = context.async();
    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value())
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

    String expectedHoldingsId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), new JsonObject().put("id", expectedHoldingsId).encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value())
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
      .withName("Create holdings")
      .withIncomingRecordType(MARC_HOLDINGS)
      .withExistingRecordType(HOLDINGS);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(mappingProfile.getId())
      .withContentType(MAPPING_PROFILE)
      .withContent(mappingProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForProfile() {
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create holdings")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.HOLDINGS);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }
}
