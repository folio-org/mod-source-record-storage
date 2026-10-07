package org.folio.services;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.UPDATE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.services.MarcBibUpdateModifyEventHandlerTest.getParsedContentWithoutLeaderAndDate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.dao.util.executor.PgPoolQueryExecutor;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.handlers.actions.MarcAuthorityUpdateModifyEventHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MarcAuthorityUpdateModifyEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String MATCHED_MARC_AUTHORITY_KEY = "MATCHED_MARC_AUTHORITY";
  private static final int CACHE_EXPIRATION_TIME = 3600;

  private static String recordId = "eae222e8-70fd-4422-852c-60d22bae36b8";
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  @RegisterExtension
  WireMockExtension mockServer = WireMockExtension.newInstance()
    .configureStaticDsl(true)
    .options(WireMockConfiguration.wireMockConfig().dynamicPort().notifier(new Slf4jNotifier(true)))
    .build();

  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;
  private RecordService recordService;
  private MarcAuthorityUpdateModifyEventHandler modifyRecordEventHandler;
  private Snapshot snapshotForRecordUpdate;
  private Record marcRecord;

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Authority")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Authority")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY);

  private MarcMappingDetail marcMappingDetail = new MarcMappingDetail()
    .withOrder(0)
    .withAction(MarcMappingDetail.Action.EDIT)
    .withField(new MarcField()
      .withField("856")
      .withIndicator1(null)
      .withIndicator2(null)
      .withSubfields(Collections.singletonList(new MarcSubfield()
        .withSubfield("u")
        .withSubaction(MarcSubfield.Subaction.INSERT)
        .withPosition(MarcSubfield.Position.BEFORE_STRING)
        .withData(new Data().withText("http://libproxy.smith.edu?url=")))));

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Authority")
    .withIncomingRecordType(MARC_AUTHORITY)
    .withExistingRecordType(MARC_AUTHORITY)
    .withMappingDetails(new MappingDetail()
      .withMarcMappingDetails(Collections.singletonList(marcMappingDetail)));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(jobProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  @BeforeAll
  static void setUpClassMarcAuthorityUpdateModify() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(PARSED_CONTENT);
  }

  @BeforeEach
  void setUp(Vertx injectedVertx, VertxTestContext testContext) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))))));

    ConsortiumConfigurationCache consortiumConfigCache = new ConsortiumConfigurationCache(injectedVertx, CACHE_EXPIRATION_TIME);
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    recordService = new RecordServiceImpl(recordDao, consortiumConfigCache);
    MappingParametersSnapshotCache mappingParametersCache = new MappingParametersSnapshotCache(injectedVertx, CACHE_EXPIRATION_TIME);
    modifyRecordEventHandler = new MarcAuthorityUpdateModifyEventHandler(recordService, null, mappingParametersCache);

    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    snapshotForRecordUpdate = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    marcRecord = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(Record.RecordType.MARC_AUTHORITY)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityId(UUID.randomUUID().toString())
        .withAuthorityHrid("hrid00123"));

    PgPoolQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    SnapshotDaoUtil.save(queryExecutor, snapshot)
      .compose(v -> recordService.saveRecord(marcRecord, okapiHeaders))
      .compose(v -> SnapshotDaoUtil.save(queryExecutor, snapshotForRecordUpdate))
      .onComplete(testContext.succeedingThenComplete());
  }

  @AfterEach
  void tearDown(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldUpdateMatchedMarcRecordWithFieldFromIncomingRecord(VertxTestContext testContext) {
    // given
    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00134nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"ybp7406512\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"eae222e8-70fd-4422-852c-60d22bae36b8\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(incomingParsedContent));
    marcRecord.getParsedRecord().setContent(Json.encode(marcRecord.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_AUTHORITY.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_AUTHORITY_KEY, Json.encode(marcRecord));
    payloadContext.put(PERMISSIONS, StringUtils.EMPTY);

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().getFirst()
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when / then
    vertx.runOnContext(v -> {
      CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

      future.whenComplete((eventPayload, throwable) -> testContext.verify(() -> {
        assertNull(throwable);
        assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_UPDATED.value(), eventPayload.getEventType());

        Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_AUTHORITY.value()), Record.class);
        assertEquals(getParsedContentWithoutLeaderAndDate(expectedParsedContent),
          getParsedContentWithoutLeaderAndDate(actualRecord.getParsedRecord().getContent().toString()));
        assertEquals(Record.State.ACTUAL, actualRecord.getState());
        assertEquals(dataImportEventPayload.getJobExecutionId(), actualRecord.getSnapshotId());
        validate005Field(expectedDate, actualRecord);
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldUpdateMatchedMarcRecordAndRetain003FieldFromIncomingRecord(VertxTestContext testContext) {
    // given
    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"003\":\"DLC\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00134nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"003\":\"DLC\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"eae222e8-70fd-4422-852c-60d22bae36b8\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(incomingParsedContent));
    marcRecord.getParsedRecord().setContent(Json.encode(marcRecord.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_AUTHORITY_KEY, Json.encode(marcRecord));
    payloadContext.put(PERMISSIONS, StringUtils.EMPTY);

    mappingProfile.withMappingDetails(new MappingDetail().withMarcMappingOption(UPDATE));
    profileSnapshotWrapper.getChildSnapshotWrappers().getFirst()
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when / then
    vertx.runOnContext(v -> modifyRecordEventHandler.handle(dataImportEventPayload)
      .whenComplete((eventPayload, throwable) -> testContext.verify(() -> {
      assertNull(throwable);
      assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_UPDATED.value(), eventPayload.getEventType());

      Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(EntityType.MARC_AUTHORITY.value()), Record.class);
      assertEquals(getParsedContentWithoutLeaderAndDate(expectedParsedContent),
        getParsedContentWithoutLeaderAndDate(actualRecord.getParsedRecord().getContent().toString()));
      assertEquals(Record.State.ACTUAL, actualRecord.getState());
      assertEquals(dataImportEventPayload.getJobExecutionId(), actualRecord.getSnapshotId());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFailedFutureWhenHasNoMarcRecord() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class,
      () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    assertTrue(isEligible);
  }

  @Test
  void shouldReturnTrueWhenHandlerIsEligibleForUpdateMarcAuthorityActionProfile() {
    // given
    ActionProfile actionProfile1 = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Update marc authority")
      .withAction(ActionProfile.Action.UPDATE)
      .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY);

    ProfileSnapshotWrapper profileSnapshotWrapper1 = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile1.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile1);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper1)
      .withCurrentNode(profileSnapshotWrapper1);

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    assertTrue(isEligible);
  }

  @Test
  void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile actionProfile1 = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper1 = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile1.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile1);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper1)
      .withCurrentNode(profileSnapshotWrapper1);

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    assertFalse(isEligible);
  }
}
