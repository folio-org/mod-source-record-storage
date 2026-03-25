package org.folio.verticle.consumers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.util.Collections.singletonList;
import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.consumers.DataImportKafkaHandler.PROFILE_SNAPSHOT_ID_KEY;
import static org.folio.consumers.ParsedRecordChunksKafkaHandler.JOB_EXECUTION_ID_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_DELETE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.services.MarcBibUpdateModifyEventHandlerTest.getParsedContentWithoutLeaderAndDate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.folio.ActionProfile;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.executor.PgPoolQueryExecutor;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.dataimport.util.RestUtil;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.Event;
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
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class DataImportConsumersVerticleTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT =
    "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

  private static final String INCORRECT_PARSED_CONTENT_WITHOUT_001 =
    "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private final String snapshotId = UUID.randomUUID().toString();
  private final String recordId = UUID.randomUUID().toString();
  private final String incorrectRecordId = UUID.randomUUID().toString();
  private Snapshot snapshotForRecordUpdate;
  private final MarcMappingDetail marcMappingDetail = new MarcMappingDetail()
    .withOrder(0)
    .withAction(MarcMappingDetail.Action.EDIT)
    .withField(new MarcField()
      .withField("856")
      .withIndicator1(null)
      .withIndicator2(null)
      .withSubfields(singletonList(new MarcSubfield()
        .withSubfield("u")
        .withSubaction(MarcSubfield.Subaction.INSERT)
        .withPosition(MarcSubfield.Position.BEFORE_STRING)
        .withData(new Data().withText("http://libproxy.smith.edu?url=")))));
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  private Record record;
  private Record incorrectRecord;

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.openMocks(this);
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))))));

    RawRecord rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(PARSED_CONTENT);

    RawRecord incorrectRawRecord = new RawRecord().withId(incorrectRecordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord incorrectParsedRecord = new ParsedRecord().withId(incorrectRecordId)
      .withContent(INCORRECT_PARSED_CONTENT_WITHOUT_001);

    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(snapshotId)
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
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("hrid00001"));

    incorrectRecord = new Record()
      .withId(incorrectRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(incorrectRecordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(incorrectRawRecord)
      .withParsedRecord(incorrectParsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("incorrectHrid"));

    PgPoolQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    RecordDaoImpl recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    SnapshotDaoUtil.save(queryExecutor, snapshot)
      .compose(v -> recordDao.saveRecord(record, okapiHeaders))
      .compose(v -> recordDao.saveRecord(incorrectRecord, okapiHeaders))
      .compose(v -> SnapshotDaoUtil.save(queryExecutor, snapshotForRecordUpdate))
      .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void shouldUpdateRecordWhenPayloadContainsUpdateMarcBibActionInCurrentNode() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfile.DataType.MARC)).getMap())
      .withChildSnapshotWrappers(singletonList(
        new ProfileSnapshotWrapper()
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(new ActionProfile()
            .withId(UUID.randomUUID().toString())
            .withAction(UPDATE)
            .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC)).getMap())
          .withChildSnapshotWrappers(singletonList(
            new ProfileSnapshotWrapper()
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(new MappingProfile()
                .withId(UUID.randomUUID().toString())
                .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
                .withExistingRecordType(MARC_BIBLIOGRAPHIC)
                .withMappingDetails(new MappingDetail()
                  .withMarcMappingOption(MappingDetail.MarcMappingOption.UPDATE)
                  .withMarcMappingDetails(List.of(marcMappingDetail)))).getMap())))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern("/linking-rules/.*"), true))
      .willReturn(WireMock.ok().withBody("[]")));

    String expectedDate = get005FieldExpectedDate();
    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent =
      "{\"leader\":\"00134nam  22000611a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"035\":{\"subfields\":[{\"a\":\"ybp7406512\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"s\":\"%s\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}"
        .formatted(record.getMatchedId());
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
        put("MATCHED_" + MARC_BIBLIOGRAPHIC.value(), Json.encode(record.withSnapshotId(snapshotForRecordUpdate.getJobExecutionId())));
        put(PROFILE_SNAPSHOT_ID_KEY, profileSnapshotWrapper.getId());
      }});

    // when
    send(DI_SRS_MARC_BIB_RECORD_CREATED,  eventPayload);

    // then
    var value = DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
    String observeTopic = getTopicName(value);
    var observedRecord = getKafkaEvent(observeTopic);

    Event obtainedEvent = Json.decodeValue(observedRecord.value(), Event.class);
    DataImportEventPayload dataImportEventPayload =
      Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value(),
      dataImportEventPayload.getEventType());

    Record actualRecord =
      Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
    assertEquals(getParsedContentWithoutLeaderAndDate(expectedParsedContent),
      getParsedContentWithoutLeaderAndDate(actualRecord.getParsedRecord().getContent().toString()));
    assertEquals(Record.State.ACTUAL, actualRecord.getState());
    assertEquals(dataImportEventPayload.getJobExecutionId(), actualRecord.getSnapshotId());
    validate005Field(expectedDate, actualRecord);
    assertNotNull(observedRecord.headers().lastHeader(RECORD_ID_HEADER));
  }

  @Test
  public void shouldBeSentDiErrorMessageWhenIncomingRecordDoesNotContainField001() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfile.DataType.MARC)).getMap())
      .withChildSnapshotWrappers(singletonList(
        new ProfileSnapshotWrapper()
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(new ActionProfile()
            .withId(UUID.randomUUID().toString())
            .withAction(UPDATE)
            .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC)).getMap())
          .withChildSnapshotWrappers(singletonList(
            new ProfileSnapshotWrapper()
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(new MappingProfile()
                .withId(UUID.randomUUID().toString())
                .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
                .withExistingRecordType(MARC_BIBLIOGRAPHIC)
                .withMappingDetails(new MappingDetail()
                  .withMarcMappingOption(MappingDetail.MarcMappingOption.UPDATE)
                  .withMarcMappingDetails(List.of(marcMappingDetail)))).getMap())))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern("/linking-rules/.*"), true))
      .willReturn(WireMock.ok().withBody("[]")));

    String incomingParsedContent =
      "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var instanceId = UUID.randomUUID().toString();
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
        put("MATCHED_" + MARC_BIBLIOGRAPHIC.value(), Json.encode(incorrectRecord.withSnapshotId(snapshotForRecordUpdate.getJobExecutionId())));
        put(PROFILE_SNAPSHOT_ID_KEY, profileSnapshotWrapper.getId());
      }});

    // when
    send(DI_SRS_MARC_BIB_RECORD_CREATED, eventPayload);

    // then
    var value = DI_ERROR.value();
    String observeTopic = getTopicName(value);
    var observedRecord = getKafkaEvent(observeTopic);

    Event obtainedEvent = Json.decodeValue(observedRecord.value(), Event.class);
    DataImportEventPayload dataImportEventPayload =
      Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_ERROR.value(), dataImportEventPayload.getEventType());
    assertTrue(dataImportEventPayload.getContext().get("ERROR")
      .contains("HRID (001 field) is not found in the incoming record."));
  }

  @Test
  public void shouldDeleteMarcAuthorityRecord() {
    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(new JobProfile()
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfile.DataType.MARC)).getMap())
      .withChildSnapshotWrappers(List.of(
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withContentType(ACTION_PROFILE)
          .withOrder(0)
          .withContent(JsonObject.mapFrom(new ActionProfile()
            .withId(UUID.randomUUID().toString())
            .withAction(DELETE)
            .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY)).getMap())
      ));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("MATCHED_MARC_AUTHORITY", Json.encode(record));
    payloadContext.put(PROFILE_SNAPSHOT_ID_KEY, profileSnapshotWrapper.getId());
    var eventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withJobExecutionId(snapshotId)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    // when
    send(DI_MARC_FOR_DELETE_RECEIVED, eventPayload);

    String observeTopic = getTopicName(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.name());
    var observedRecord = getKafkaEvent(observeTopic);
    Event obtainedEvent = Json.decodeValue(observedRecord.value(), Event.class);
    var resultPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value(), resultPayload.getEventType());
    assertEquals(record.getExternalIdsHolder().getAuthorityId(), resultPayload.getContext().get("AUTHORITY_RECORD_ID"));
    assertEquals(ACTION_PROFILE, resultPayload.getCurrentNode().getContentType());
  }

  private String getTopicName(String value) {
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, value);
  }

  private void send(DataImportEventTypes eventType, DataImportEventPayload eventPayload) {
    var topic = getTopicName(eventType.value());
    var event = new Event().withEventPayload(Json.encode(eventPayload));
    Map<String, String> headers = Map.of(
        RestUtil.OKAPI_URL_HEADER, mockServer.baseUrl(),
        RestUtil.OKAPI_TENANT_HEADER, TENANT_ID,
        RestUtil.OKAPI_TOKEN_HEADER, TOKEN,
        JOB_EXECUTION_ID_HEADER, snapshotId,
        RECORD_ID_HEADER, record.getId(),
        CHUNK_ID_HEADER, UUID.randomUUID().toString()
    );
    send(topic, "1", Json.encode(event), headers);
  }
}
