package org.folio.services.handlers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.services.util.AdditionalFieldsUtil;
import org.junit.jupiter.api.Test;

public class HoldingsPostProcessingEventHandlerTest extends AbstractPostProcessingEventHandlerTest {

  @Override
  protected Record.RecordType getMarcType() {
    return MARC_HOLDING;
  }

  @Override
  protected AbstractPostProcessingEventHandler createHandler(RecordService recordService, SnapshotService snapshotService, KafkaConfig kafkaConfig) {
    return new HoldingsPostProcessingEventHandler(recordService, snapshotService, kafkaConfig, mappingParametersCache);
  }

  @Test
  void shouldSetHoldingsIdToRecord(VertxTestContext testContext) {
    String expectedHoldingsId = UUID.randomUUID().toString();
    String expectedHrId = UUID.randomUUID().toString();

    JsonObject holdings = createExternalEntity(expectedHoldingsId, expectedHrId);

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), holdings.encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
      .onFailure(future::completeExceptionally)
      .onSuccess(marcRecord -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, e) -> {
      if (e != null) {
        testContext.failNow(e);
        return;
      }
      recordDao.getRecordByMatchedId(RECORD_ID, TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          testContext.failNow(getAr.cause());
          return;
        }

        assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        assertNotNull(updatedRecord.getExternalIdsHolder());
        assertEquals(expectedHoldingsId, updatedRecord.getExternalIdsHolder().getHoldingsId());

        assertNotNull(updatedRecord.getParsedRecord());
        assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        assertTrue(!fields.isEmpty());

        String actualHoldingsId = getInventoryId(fields);
        assertEquals(expectedHoldingsId, actualHoldingsId);
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldSaveRecordWhenRecordDoesntExist(VertxTestContext testContext) throws IOException {
    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Record defaultRecord = new Record()
      .withId(recordId)
      .withSnapshotId(snapshotId1)
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

    vertx.runOnContext(v -> handler.handle(dataImportEventPayload)
      .whenComplete((payload, e) -> {
      if (e != null) {
        testContext.failNow(e);
        return;
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          testContext.failNow(getAr.cause());
          return;
        }

        assertTrue(getAr.result().isPresent());
        Record savedRecord = getAr.result().get();

        assertNotNull(savedRecord.getExternalIdsHolder());
        assertEquals(expectedHoldingsId, savedRecord.getExternalIdsHolder().getHoldingsId());

        assertNotNull(savedRecord.getParsedRecord());
        assertNotNull(savedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(savedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        assertTrue(!fields.isEmpty());

        String actualHoldingsId = getInventoryId(fields);
        assertEquals(expectedHoldingsId, actualHoldingsId);
        testContext.completeNow();
      });
    }));
  }

  @Test
  void shouldSetHoldingsIdToParsedRecordWhenContentHasField999(VertxTestContext testContext) {
    record.withParsedRecord(new ParsedRecord()
      .withId(RECORD_ID)
      .withContent(PARSED_CONTENT_WITH_999_FIELD));

    String expectedHoldingsId = UUID.randomUUID().toString();
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(HOLDINGS.value(), new JsonObject().put("id", expectedHoldingsId).encode());
    payloadContext.put(MARC_HOLDINGS.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload =
      createDataImportEventPayload(payloadContext, DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING);

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
      .onFailure(future::completeExceptionally)
      .onSuccess(rec -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        testContext.failNow(throwable);
        return;
      }
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          testContext.failNow(getAr.cause());
          return;
        }
        assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        assertNotNull(updatedRecord.getExternalIdsHolder());
        assertEquals(expectedHoldingsId, updatedRecord.getExternalIdsHolder().getHoldingsId());

        assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        assertFalse(fields.isEmpty());

        String actualHoldingsId = getInventoryId(fields);
        assertEquals(expectedHoldingsId, actualHoldingsId);
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldUpdateField005WhenThisFiledIsNotProtected(VertxTestContext testContext) throws IOException {
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

    vertx.runOnContext(v -> handler.handle(dataImportEventPayload)
      .whenComplete((payload, throwable) -> {
      if (throwable != null) {
        testContext.failNow(throwable);
        return;
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          testContext.failNow(getAr.cause());
          return;
        }

        assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        validate005Field(expectedDate, updatedRecord);

        testContext.completeNow();
      });
    }));
  }

  @Test
  void shouldUpdateField005WhenThisFiledIsProtected(VertxTestContext testContext) throws IOException {
    MappingParameters mappingParameters = new MappingParameters()
      .withMarcFieldProtectionSettings(List.of(new MarcFieldProtectionSetting()
        .withField(TAG_005)
        .withData("*")));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
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

    vertx.runOnContext(v -> handler.handle(dataImportEventPayload)
      .whenComplete((payload, throwable) -> {
      if (throwable != null) {
        testContext.failNow(throwable);
        return;
      }
      recordDao.getRecordByMatchedId(recordId, TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          testContext.failNow(getAr.cause());
          return;
        }

        assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        String actualDate = AdditionalFieldsUtil.getValueFromControlledField(updatedRecord, TAG_005);
        assertEquals(expectedDate, actualDate);

        testContext.completeNow();
      });
    }));
  }

  @Test
  void shouldSetHoldingsHridToParsedRecordWhenContentHasNotField001(VertxTestContext testContext) {
    record.withParsedRecord(new ParsedRecord()
      .withId(RECORD_ID)
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
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
      .onFailure(future::completeExceptionally)
      .onSuccess(rec -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, throwable) -> {
      if (throwable != null) {
        testContext.failNow(throwable);
        return;
      }
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(getAr -> {
        if (getAr.failed()) {
          testContext.failNow(getAr.cause());
          return;
        }
        assertTrue(getAr.result().isPresent());
        Record updatedRecord = getAr.result().get();

        assertNotNull(updatedRecord.getExternalIdsHolder());
        assertEquals(expectedHoldingsId, updatedRecord.getExternalIdsHolder().getHoldingsId());

        assertNotNull(updatedRecord.getParsedRecord().getContent());
        JsonObject parsedContent = JsonObject.mapFrom(updatedRecord.getParsedRecord().getContent());

        JsonArray fields = parsedContent.getJsonArray("fields");
        assertTrue(!fields.isEmpty());

        String actualHoldingsHrid = getInventoryHrid(fields);
        assertEquals(expectedHoldingsHrid, actualHoldingsHrid);
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldReturnFailedFutureWhenHoldingsOrRecordDoesNotExist(VertxTestContext testContext) {
    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext);

    CompletableFuture<DataImportEventPayload> future = handler.handle(dataImportEventPayload);

    future.whenComplete((payload, throwable) -> {
      assertNotNull(throwable);
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnFailedFutureWhenParsedRecordHasNoFields(VertxTestContext testContext) {
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
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordDao.saveRecord(record, okapiHeaders)
      .onFailure(future::completeExceptionally)
      .onSuccess(marcRecord -> handler.handle(dataImportEventPayload)
        .thenApply(future::complete)
        .exceptionally(future::completeExceptionally));

    future.whenComplete((payload, throwable) -> {
      assertNotNull(throwable);
      testContext.completeNow();
    });
  }

  @Test
  void shouldReturnTrueWhenHandlerIsEligibleForProfile() {
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

    assertTrue(isEligible);
  }

  @Test
  void shouldReturnFalseWhenHandlerIsNotEligibleForProfile() {
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

    assertFalse(isEligible);
  }
}
