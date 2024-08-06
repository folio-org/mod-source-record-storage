package org.folio.services;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.UPDATE;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.VerificationException;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
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
import java.util.concurrent.TimeoutException;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.JobProfile;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.client.InstanceLinkClient;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.SnapshotDao;
import org.folio.dao.SnapshotDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.services.caches.LinkingRulesCache;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.exceptions.DuplicateRecordException;
import org.folio.services.handlers.actions.MarcBibUpdateModifyEventHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class MarcBibUpdateModifyEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT =
    "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private static final String MAPPING_METADATA__URL = "/mapping-metadata";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private static final String USER_ID_HEADER = "userId";
  private static final String INSTANCE_LINKS_URL = "/links/instances";
  private static final UrlPathPattern URL_PATH_PATTERN =
    new UrlPathPattern(new RegexPattern(INSTANCE_LINKS_URL + "/.*"), true);
  private static final String LINKING_RULES_URL = "/linking-rules/instance-authority";
  private static final String CENTRAL_TENANT_ID = "centralTenantId";
  private static final String SECOND_PARSED_CONTENT =
    "{\"leader\":\"02326cam a2200301Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
      "{\"100\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author.\"},{\"0\":\"http://id.loc.gov/authorities/names/n2008052404\"},{\"9\":\"5a56ffa8-e274-40ca-8620-34a23b5b45dd\"}]}}]}";
  private static final String instanceId = UUID.randomUUID().toString();
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final String recordId = "eae222e8-70fd-4422-852c-60d22bae36b8";
  private static final String userId = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;
  private SnapshotDao snapshotDao;
  private RecordService recordService;
  private SnapshotService snapshotService;
  private MarcBibUpdateModifyEventHandler modifyRecordEventHandler;
  private Snapshot snapshotForRecordUpdate;
  private Record record;
  private Snapshot snapshot;
  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);
  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Bibs")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);
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
  private List<MarcMappingDetail> marcBibMappingDetail = List.of(
    new MarcMappingDetail().withOrder(0).withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("015").withIndicator1("*").withIndicator2("*")
        .withSubfields(Collections.singletonList(
          new MarcSubfield().withSubfield("*").withData(null).withSubaction(null).withPosition(null)))),
    new MarcMappingDetail().withOrder(1).withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("016").withIndicator1("*").withIndicator2("*")
        .withSubfields(Collections.singletonList(
          new MarcSubfield().withSubfield("*").withData(null).withSubaction(null).withPosition(null)))),
    new MarcMappingDetail().withOrder(2).withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField().withField("035").withIndicator1(null).withIndicator2(null)
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("a").withSubaction(MarcSubfield.Subaction.REPLACE).withPosition(null)
          .withData(new Data().withText(null).withFind("(OCoLC)on").withReplaceWith("(OCoLC)")))))
  );
  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Bibs")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
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
  private MappingProfile updateMappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update MARC Bibs")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMappingDetails(new MappingDetail());
  private MappingProfile marcBibModifyMappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMappingDetails(new MappingDetail()
      .withMarcMappingDetails(marcBibMappingDetail)
      .withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY));

  @BeforeClass
  public static void setUpBeforeClass(TestContext context) throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(PARSED_CONTENT);
    Async async = context.async();
    TenantClient tenantClient = new TenantClient(OKAPI_URL, CENTRAL_TENANT_ID, TOKEN);
    try {
      tenantClient.postTenant(new TenantAttributes().withModuleTo("3.2.0"), res2 -> {
        if (res2.result().statusCode() == 204) {
          return;
        }
        if (res2.result().statusCode() == 201) {
          tenantClient.getTenantByOperationId(res2.result().bodyAsJson(TenantJob.class).getId(), 60000, context.asyncAssertSuccess(res3 -> {
            context.assertTrue(res3.bodyAsJson(TenantJob.class).getComplete());
            String error = res3.bodyAsJson(TenantJob.class).getError();
            if (error != null) {
              context.assertTrue(error.contains("EventDescriptor was not registered for eventType"));
            }
          }));
        } else {
          context.assertEquals("Failed to make post tenant. Received status code 400", res2.result().bodyAsString());
        }
        async.complete();
      });
    } catch (Exception e) {
      e.printStackTrace();
      async.complete();
    }
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.openMocks(this);
    wireMockServer.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA__URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))))));

    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    snapshotDao = new SnapshotDaoImpl(postgresClientFactory);
    recordService = new RecordServiceImpl(recordDao);
    snapshotService = new SnapshotServiceImpl(snapshotDao);
    InstanceLinkClient instanceLinkClient = new InstanceLinkClient();
    LinkingRulesCache linkingRulesCache = new LinkingRulesCache(instanceLinkClient, vertx);
    modifyRecordEventHandler =
      new MarcBibUpdateModifyEventHandler(recordService, snapshotService, new MappingParametersSnapshotCache(vertx), vertx,
        instanceLinkClient, linkingRulesCache);

    snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    Snapshot snapshot_2 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    snapshotForRecordUpdate = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()))
      .withMetadata(new Metadata());

    Record record_2 = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(UUID.randomUUID().toString())
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()))
      .withMetadata(new Metadata());

    ReactiveClassicGenericQueryExecutor queryExecutorLocalTenant = postgresClientFactory.getQueryExecutor(TENANT_ID);
    ReactiveClassicGenericQueryExecutor queryExecutorCentralTenant = postgresClientFactory.getQueryExecutor(CENTRAL_TENANT_ID);

    SnapshotDaoUtil.save(queryExecutorLocalTenant, snapshot)
      .compose(v -> recordService.saveRecord(record, Map.of(OKAPI_TENANT_HEADER, TENANT_ID)))
      .compose(v -> SnapshotDaoUtil.save(queryExecutorLocalTenant, snapshotForRecordUpdate))
      .compose(v -> SnapshotDaoUtil.save(queryExecutorCentralTenant, snapshot_2))
      .compose(v -> recordService.saveRecord(record_2, Map.of(OKAPI_TENANT_HEADER, CENTRAL_TENANT_ID)))
      .onComplete(context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    wireMockServer.resetRequests();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(ar -> {
        if (ar.failed()) {
          context.asyncAssertFailure();
        } else {
          SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(CENTRAL_TENANT_ID))
            .onComplete(arCentral -> {
              if (arCentral.failed()) {
                context.asyncAssertFailure();
              } else {
                context.asyncAssertSuccess();
              }
            });
        }
      });
  }

  @Test
  public void shouldUpdateMarcRecordOnCentralTenantIfCentralTenantIdIsInContext(TestContext context) {
    // given
    Async async = context.async();

    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00134nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"ybp7406512\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"eae222e8-70fd-4422-852c-60d22bae36b8\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record.withSnapshotId(snapshotForRecordUpdate.getJobExecutionId())));
    payloadContext.put("CENTRAL_TENANT_ID", CENTRAL_TENANT_ID);

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(wireMockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(record.getSnapshotId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withAdditionalProperty(USER_ID_HEADER, userId);

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), eventPayload.getEventType());

      Record actualRecord =
        Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(getParsedContentWithoutLeaderAndDate(expectedParsedContent),
        getParsedContentWithoutLeaderAndDate(actualRecord.getParsedRecord().getContent().toString()));
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      context.assertEquals(dataImportEventPayload.getJobExecutionId(), actualRecord.getSnapshotId());
      validate005Field(context, expectedDate, actualRecord);
      recordService.getRecordById(actualRecord.getId(), CENTRAL_TENANT_ID)
        .onComplete(ar -> {
          context.assertTrue(ar.succeeded());
          context.assertTrue(ar.result().isPresent());
          validate005Field(context, expectedDate, actualRecord);
          snapshotService.getSnapshotById(record.getSnapshotId(), CENTRAL_TENANT_ID)
            .onComplete(ar2 -> {
              context.assertTrue(ar2.succeeded());
              context.assertTrue(ar2.result().isPresent());
              context.assertEquals(ar2.result().get().getJobExecutionId(), record.getSnapshotId());
              async.complete();
            });
        });
    });
  }

  @Test
  public void shouldUpdateMatchedMarcRecordWithFieldFromIncomingRecord(TestContext context) {
    // given
    Async async = context.async();

    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00134nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"ybp7406512\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"eae222e8-70fd-4422-852c-60d22bae36b8\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(wireMockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), eventPayload.getEventType());

      Record actualRecord =
        Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(getParsedContentWithoutLeaderAndDate(expectedParsedContent),
        getParsedContentWithoutLeaderAndDate(actualRecord.getParsedRecord().getContent().toString()));
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      context.assertEquals(dataImportEventPayload.getJobExecutionId(), actualRecord.getSnapshotId());
      validate005Field(context, expectedDate, actualRecord);
      async.complete();
    });
  }

  @Test
  public void shouldUpdateMarcRecordAndCreate035FieldAndRemove003Field(TestContext context) {
    // given
    Async async = context.async();

    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"2300089\"},{\"003\":\"LTSCA\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00167nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"(LTSCA)2300089\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    updateMappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(updateMappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(updateMappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(wireMockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), eventPayload.getEventType());

      Record actualRecord =
        Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(expectedParsedContent, getParsedContentWithoutDate(actualRecord.getParsedRecord().getContent().toString()));
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      validate005Field(context, expectedDate, actualRecord);
      async.complete();
    });
  }

  @Test
  public void shouldUpdateMarcRecordAndCreate035FieldAndRemove035withDuplicateHrIdAndRemove003Field(
    TestContext context) {
    // given
    Async async = context.async();

    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"2300089\"},{\"003\":\"LTSCA\"},{\"035\":{\"subfields\":[{\"a\":\"ybp7406411\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00167nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"(LTSCA)2300089\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    updateMappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(updateMappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(updateMappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(wireMockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), eventPayload.getEventType());

      Record actualRecord =
        Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(expectedParsedContent, getParsedContentWithoutDate(actualRecord.getParsedRecord().getContent().toString()));
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      validate005Field(context, expectedDate, actualRecord);
      async.complete();
    });
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord()
    throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForModifyMarcBibActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Modify marc bib")
      .withAction(MODIFY)
      .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);

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

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertFalse(isEligible);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForUpdateMarcBibActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
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

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertFalse(isEligible);
  }

  @Test
  public void shouldNotUpdateLinksWhenIncomingZeroSubfieldIsSameAsExisting(TestContext context) {
    // given
    String incomingParsedContent =
      "{\"leader\":\"02340cam a2200301Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
        "{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"},{\"0\":\"http://id.loc.gov/authorities/names/n2008052404\"},{\"9\":\"5a56ffa8-e274-40ca-8620-34a23b5b45dd\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00220cam a2200061Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
        "{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"},{\"0\":\"http://id.loc.gov/authorities/names/n2008052404\"},{\"9\":\"5a56ffa8-e274-40ca-8620-34a23b5b45dd\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";

    verifyBibRecordUpdate(incomingParsedContent, expectedParsedContent, 1, 0, context);
  }

  @Test
  public void shouldUpdateLinksWhenIncomingZeroSubfieldIsNull(TestContext context) {
    // given
    String incomingParsedContent =
      "{\"leader\":\"02340cam a2200301Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
        "{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00133cam a2200061Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
        "{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";

    verifyBibRecordUpdate(incomingParsedContent, expectedParsedContent, 1, 1, context);
  }

  @Test
  public void shouldUnlinkBibFieldWhenIncomingZeroSubfieldIsDifferent(TestContext context) {
    // given
    String incomingParsedContent =
      "{\"leader\":\"02340cam a2200301Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
        "{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"},{\"0\":\"test different 0 subfield\"},{\"9\":\"5a56ffa8-e274-40ca-8620-34a23b5b45dd\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00160cam a2200061Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"}," +
        "{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"},{\"0\":\"test different 0 subfield\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";

    verifyBibRecordUpdate(incomingParsedContent, expectedParsedContent, 1, 1, context);
  }

  @Test
  public void shouldNotUpdateBibFieldWhen500ErrorGetEntityLinkRequest(TestContext context) {
    wireMockServer.stubFor(get(URL_PATH_PATTERN).willReturn(WireMock.serverError()));
    String incomingParsedContent = "{\"leader\":\"02340cam a2200301Ki 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"100\":{\"subfields\":[{\"a\":\"Chin, Staceyann Test,\"},{\"e\":\"author updated.\"},{\"0\":\"test different 0 subfield\"},{\"9\":\"5a56ffa8-e274-40ca-8620-34a23b5b45dd\"}],\"ind1\":\"1\",\"ind2\":\" \"}}]}";

    Async async = context.async();
    Snapshot snapshotForRecordUpdate = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    Snapshot secondSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    String secondRecordId = UUID.randomUUID().toString();
    ParsedRecord secondParsedRecord = new ParsedRecord().withId(secondRecordId).withContent(SECOND_PARSED_CONTENT);
    RawRecord secondRawRecord = new RawRecord().withId(secondRecordId).withContent("test content");

    Record secondRecord = new Record()
      .withId(secondRecordId)
      .withSnapshotId(secondSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(secondRecordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(secondRawRecord)
      .withParsedRecord(secondParsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId))
      .withMetadata(new Metadata());

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), secondSnapshot)
      .compose(v -> recordService.saveRecord(secondRecord, okapiHeaders))
      .compose(v -> SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshotForRecordUpdate))
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(result -> {
        Record incomingRecord = new Record().withId(secondRecord.getId())
          .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
          .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
        secondRecord.getParsedRecord().setContent(Json.encode(secondRecord.getParsedRecord().getContent()));
        HashMap<String, String> payloadContext = new HashMap<>();
        payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
        payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(secondRecord));

        updateMappingProfile.getMappingDetails().withMarcMappingOption(UPDATE)
          .withMarcMappingDetails(singletonList(new MarcMappingDetail()
            .withOrder(0)
            .withField(new MarcField()
              .withField("100")
              .withIndicator1("*")
              .withIndicator2("*")
              .withSubfields(
                List.of(new MarcSubfield().withSubfield("e"), new MarcSubfield().withSubfield("0"))))));
        profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
          .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
            .withProfileId(updateMappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(updateMappingProfile).getMap())));

        var dataImportEventPayload = new DataImportEventPayload()
          .withTenant(TENANT_ID)
          .withOkapiUrl(wireMockServer.baseUrl())
          .withToken(TOKEN)
          .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
          .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
          .withContext(payloadContext)
          .withProfileSnapshot(profileSnapshotWrapper)
          .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
        modifyRecordEventHandler.handle(dataImportEventPayload)
          .whenComplete((eventPayload, throwable) -> {
            context.assertNotNull(throwable);
            context.assertNull(eventPayload);
            context.assertTrue(throwable.getMessage().contains(String.format("Error loading InstanceLinkDtoCollection by instanceId: '%s'", instanceId)));
            verifyGetAndPut(context, 1, 0);
            async.complete();
          });
      });
  }

  @Test
  public void shouldReturnExceptionForDuplicateRecord(TestContext context) {
    // given
    Async async = context.async();

    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00134nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"ybp7406512\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"eae222e8-70fd-4422-852c-60d22bae36b8\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));

    HashMap<String, String> payloadContextOriginalRecord = new HashMap<>();
    payloadContextOriginalRecord.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContextOriginalRecord.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    HashMap<String, String> payloadContextDuplicateRecord = new HashMap<>();
    payloadContextDuplicateRecord.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContextDuplicateRecord.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayloadOriginalRecord = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(wireMockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContextOriginalRecord)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    DataImportEventPayload dataImportEventPayloadDuplicateRecord = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(wireMockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContextDuplicateRecord)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future1 = modifyRecordEventHandler.handle(dataImportEventPayloadOriginalRecord);
    CompletableFuture<DataImportEventPayload> future2 = modifyRecordEventHandler.handle(dataImportEventPayloadDuplicateRecord);

    // then
    future1.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), eventPayload.getEventType());

      Record actualRecord =
        Json.decodeValue(dataImportEventPayloadOriginalRecord.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(getParsedContentWithoutLeaderAndDate(expectedParsedContent),
        getParsedContentWithoutLeaderAndDate(actualRecord.getParsedRecord().getContent().toString()));
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      context.assertEquals(dataImportEventPayloadOriginalRecord.getJobExecutionId(), actualRecord.getSnapshotId());
      validate005Field(context, expectedDate, actualRecord);
    });

    future2.whenComplete((eventPayload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals(throwable.getClass(), DuplicateRecordException.class);
      async.complete();
    });
  }

  private InstanceLinkDtoCollection constructLinkCollection() {
    return new InstanceLinkDtoCollection()
      .withLinks(singletonList(constructLink()));
  }

  private Link constructLink() {
    return new Link().withId(nextInt())
      .withAuthorityId(UUID.randomUUID().toString())
      .withInstanceId(UUID.randomUUID().toString())
      .withAuthorityNaturalId("n2008052404")
      .withLinkingRuleId(1);
  }

  private List<LinkingRuleDto> constructLinkingRulesCollection() {
    return singletonList(new LinkingRuleDto()
      .withId(1)
      .withBibField("100"));
  }

  private void verifyBibRecordUpdate(String incomingParsedContent, String expectedParsedContent,
                                     int getRequestCount, int putRequestCount, TestContext context) {
    wireMockServer.stubFor(get(URL_PATH_PATTERN)
      .willReturn(WireMock.ok().withBody(Json.encode(constructLinkCollection()))));
    wireMockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.ok().withBody(Json.encode(constructLinkingRulesCollection()))));
    wireMockServer.stubFor(put(URL_PATH_PATTERN).willReturn(aResponse().withStatus(202)));

    // given
    String expectedDate = get005FieldExpectedDate();
    Async async = context.async();
    Snapshot snapshotForRecordUpdate = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    Snapshot secondSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    String secondRecordId = UUID.randomUUID().toString();
    ParsedRecord secondParsedRecord = new ParsedRecord().withId(secondRecordId).withContent(SECOND_PARSED_CONTENT);
    RawRecord secondRawRecord = new RawRecord().withId(secondRecordId).withContent("test content");

    Record secondRecord = new Record()
      .withId(secondRecordId)
      .withSnapshotId(secondSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(secondRecordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(secondRawRecord)
      .withParsedRecord(secondParsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId))
      .withMetadata(new Metadata());

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), secondSnapshot)
      .compose(v -> recordService.saveRecord(secondRecord, okapiHeaders))
      .compose(v -> SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshotForRecordUpdate))
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(result -> {
        Record incomingRecord = new Record().withId(secondRecord.getId())
          .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
          .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
        secondRecord.getParsedRecord().setContent(Json.encode(secondRecord.getParsedRecord().getContent()));
        HashMap<String, String> payloadContext = new HashMap<>();
        payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
        payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(secondRecord));

        updateMappingProfile.getMappingDetails().withMarcMappingOption(UPDATE)
          .withMarcMappingDetails(emptyList());
        profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
          .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
            .withProfileId(updateMappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(updateMappingProfile).getMap())));

        var dataImportEventPayload =
          new DataImportEventPayload()
            .withTenant(TENANT_ID)
            .withOkapiUrl(wireMockServer.baseUrl())
            .withToken(TOKEN)
            .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
            .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
            .withContext(payloadContext)
            .withProfileSnapshot(profileSnapshotWrapper)
            .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
        modifyRecordEventHandler.handle(dataImportEventPayload)
          .whenComplete((eventPayload, throwable) -> {
            var actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
            context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
            context.assertEquals(DI_SRS_MARC_BIB_RECORD_UPDATED.value(), eventPayload.getEventType());
            context.assertNull(throwable);

            verifyRecords(context, getParsedContentWithoutDate(expectedParsedContent),
              getParsedContentWithoutDate(actualRecord.getParsedRecord().getContent().toString()));
            validate005Field(context, expectedDate, actualRecord);
            verifyGetAndPut(context, getRequestCount, putRequestCount);
            async.complete();
          });
      });
  }

  private void verifyGetAndPut(TestContext context, int getRequestCount, int putRequestCount) {
    try {
      wireMockServer.verify(getRequestCount, getRequestedFor(URL_PATH_PATTERN));
      wireMockServer.verify(putRequestCount, putRequestedFor(URL_PATH_PATTERN));
    } catch (VerificationException e) {
      context.fail(e);
    }
  }

  private void verifyRecords(TestContext context, String expectedParsedContent, String actualParsedContent) {
    try {
      context.assertEquals(
        mapper.readTree(expectedParsedContent),
        mapper.readTree(actualParsedContent));
    } catch (JsonProcessingException e) {
      context.fail(e);
    }
  }

  public static String getParsedContentWithoutLeaderAndDate(String parsedContent) {
    JsonObject parsedContentAsJson = new JsonObject(parsedContent);
    parsedContentAsJson.remove("leader");
    remove005FieldFromRecord(parsedContentAsJson);

    return parsedContentAsJson.encode();
  }

  public static String getParsedContentWithoutDate(String parsedContent) {
    JsonObject parsedContentAsJson = new JsonObject(parsedContent);
    remove005FieldFromRecord(parsedContentAsJson);

    return parsedContentAsJson.encode();
  }

  private static JsonObject remove005FieldFromRecord(JsonObject recordJson) {
    JsonArray fieldsArray = recordJson.getJsonArray("fields");
    for (int i = 0; i < fieldsArray.size(); i++) {
      JsonObject fieldObject = fieldsArray.getJsonObject(i);
      if (fieldObject.containsKey(TAG_005)) {
        fieldsArray.remove(i);
        break;
      }
    }
    return recordJson;
  }
}
