package org.folio.services.handlers;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.SnapshotDao;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;
import org.folio.services.SnapshotService;
import org.folio.services.SnapshotServiceImpl;
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;

public abstract class AbstractPostProcessingEventHandlerTest extends AbstractLBServiceTest {

  protected static final String PARSED_CONTENT_WITH_999_FIELD =
    "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";
  protected static final String PARSED_CONTENT_WITHOUT_001_FIELD =
    "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";
  protected static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String USER_ID = "userId";
  protected static final String RECORD_ID = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;
  protected final String snapshotId1 = UUID.randomUUID().toString();
  protected final String snapshotId2 = UUID.randomUUID().toString();
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  @Mock
  private ConsortiumConfigurationCache consortiumConfigurationCache;
  protected Record record;
  protected RecordDao recordDao;
  protected RecordService recordService;
  protected SnapshotDao snapshotDao;

  protected SnapshotService snapshotService;

  protected MappingParametersSnapshotCache mappingParametersCache;

  protected AbstractPostProcessingEventHandler handler;

  @RegisterExtension
  WireMockExtension mockServer = WireMockExtension.newInstance()
    .configureStaticDsl(true)
    .options(WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)))
    .build();

  @BeforeAll
  public static void setUpClassPostProcessing() throws IOException {
    rawRecord = new RawRecord().withId(RECORD_ID)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(RECORD_ID)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
  }

  @BeforeEach
  public void setUp(Vertx injectedVertx, VertxTestContext testContext) {
    mockServer.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))))));

    mappingParametersCache = new MappingParametersSnapshotCache(injectedVertx, 3600);
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    recordService = new RecordServiceImpl(recordDao, consortiumConfigurationCache);
    snapshotService = new SnapshotServiceImpl(snapshotDao);
    handler = createHandler(recordService, snapshotService, kafkaConfig);

    Snapshot snapshot1 = new Snapshot()
      .withJobExecutionId(snapshotId1)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    Snapshot snapshot2 = new Snapshot()
      .withJobExecutionId(snapshotId2)
      .withProcessingStartedDate(Date.from(LocalDateTime.now().plus(1, ChronoUnit.HOURS).atOffset(ZoneOffset.UTC).toInstant()))
      .withStatus(Snapshot.Status.COMMITTED);

    List<Snapshot> snapshots = new ArrayList<>();
    snapshots.add(snapshot1);
    snapshots.add(snapshot2);

    this.record = new Record()
      .withId(RECORD_ID)
      .withMatchedId(RECORD_ID)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(getMarcType())
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(null);

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshots)
      .onComplete(testContext.succeedingThenComplete());
  }

  protected abstract Record.RecordType getMarcType();

  protected abstract AbstractPostProcessingEventHandler createHandler(RecordService recordService, SnapshotService snapshotService, KafkaConfig kafkaConfig);

  @AfterEach
  public void cleanUp(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  protected DataImportEventPayload createDataImportEventPayload(HashMap<String, String> payloadContext,
                                                                DataImportEventTypes diInventoryInstanceCreatedReadyForPostProcessing) {
    return new DataImportEventPayload()
      .withContext(payloadContext)
      .withEventType(diInventoryInstanceCreatedReadyForPostProcessing.value())
      .withJobExecutionId(record.getSnapshotId())
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withAdditionalProperty(USER_ID, UUID.randomUUID().toString());
  }

  protected JsonObject createExternalEntity(String id, String hrid) {
    return new JsonObject()
      .put("id", id)
      .put("hrid", hrid);
  }

  protected String getInventoryId(JsonArray fields) {
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
    return actualInstanceId;
  }

  protected String getInventoryHrid(JsonArray fields) {
    String actualInstanceHrid = null;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      if (field.containsKey("001")) {
        actualInstanceHrid = field.getString("001");
      }
    }
    return actualInstanceHrid;
  }
}
