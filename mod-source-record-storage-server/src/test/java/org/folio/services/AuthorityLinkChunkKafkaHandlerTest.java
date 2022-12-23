package org.folio.services;

import static java.util.Collections.singletonList;
import static org.folio.consumers.AuthorityLinkChunkKafkaHandler.SRS_BIB_UPDATE_TOPIC;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.verticle.consumers.AuthorityLinkChunkConsumersVerticle.AUTHORITY_INSTANCE_LINKS_TOPIC;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.commons.lang3.RandomUtils;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.BibAuthorityLinksUpdate;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Link;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Subfield;
import org.folio.rest.jaxrs.model.SubfieldsChange;
import org.folio.rest.jaxrs.model.UpdateTarget;
import org.folio.rest.util.OkapiConnectionParams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(VertxUnitRunner.class)
public class AuthorityLinkChunkKafkaHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_MARC_RECORD_LINKED_PATH = "src/test/resources/parsedMarcRecordLinked.json";
  private static final String PARSED_MARC_RECORD_LINKED_UPDATED_PATH = "src/test/resources/parsedMarcRecordLinkedUpdated.json";
  private static final String PARSED_MARC_RECORD_UNLINKED = "src/test/resources/parsedMarcRecordUnlinked.json";
  private static final String KAFKA_KEY_NAME = "test-key";
  private static final String KAFKA_TEST_HEADER = "test";
  private static final String KAFKA_CONSUMER_TOPIC = getTopicName(AUTHORITY_INSTANCE_LINKS_TOPIC);
  private static final String KAFKA_PRODUCER_TOPIC = getTopicName(SRS_BIB_UPDATE_TOPIC);
  private static final String LINKED_AUTHORITY_ID = "6d19a8e8-2b71-482e-bfda-2b97f8722a2f";
  private static final String SECOND_RECORD_ID = UUID.randomUUID().toString();
  private static final String SECOND_INSTANCE_ID = UUID.randomUUID().toString();
  private static final Integer LINK_ID = RandomUtils.nextInt();
  private static final Map<String, String> OKAPI_HEADERS = Map.of(
    OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL,
    OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID,
    OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static String updatedParsedRecordContent;
  private static String unlinkedParsedRecordContent;

  private final String authorityJobId = UUID.randomUUID().toString();
  private final String recordId = UUID.randomUUID().toString();
  private final String instanceId = UUID.randomUUID().toString();
  private final RawRecord rawRecord = new RawRecord().withId(recordId)
    .withContent("test content");

  private RecordDao recordDao;
  private RecordService recordService;
  private Record record;
  private Record secondRecord;

  @BeforeClass
  public static void setUpClass() throws IOException {
    updatedParsedRecordContent = new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_LINKED_UPDATED_PATH)).encode();
    unlinkedParsedRecordContent = new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_UNLINKED)).encode();
  }

  @Before
  public void setUp(TestContext context) throws IOException {
    recordDao = new RecordDaoImpl(postgresClientFactory);
    recordService = new RecordServiceImpl(recordDao);

    var async = context.async();
    var snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    var parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_LINKED_PATH)).encode());
    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    var authoritySnapshot = new Snapshot()
      .withJobExecutionId(authorityJobId)
      .withProcessingStartedDate(new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)))
      .withStatus(Snapshot.Status.COMMITTED);
    var secondParsedRecord = new ParsedRecord().withId(SECOND_RECORD_ID)
      .withContent(new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_LINKED_PATH)).encode());
    secondRecord = new Record()
      .withId(SECOND_RECORD_ID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(SECOND_RECORD_ID)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(secondParsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_INSTANCE_ID));

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordService.saveRecord(record, TENANT_ID))
      .compose(savedRecord -> SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), authoritySnapshot))
      .compose(savedSnapshot -> recordService.saveRecord(secondRecord, TENANT_ID))
      .onSuccess(ar -> async.complete())
      .onFailure(context::fail);
  }

  @After
  public void cleanUp(TestContext context) {
    var async = context.async();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldUpdateBibRecordAndSendRecordUpdatedEvent(TestContext context)
    throws InterruptedException, IOException {
    var async = context.async();

    var parsedRecord = record.getParsedRecord();
    var updateTargets = buildUpdateTargets();
    var event = buildLinkEventForUpdate(updateTargets);

    var traceHeader = UUID.randomUUID().toString();
    cluster.send(createRequest(event, traceHeader));
    var keyValues = cluster.read(ReadKeyValues.from(KAFKA_PRODUCER_TOPIC)
      .withMaxTotalPollTime(60, TimeUnit.SECONDS)
      .filterOnHeaders(headers -> Arrays.equals(headers.lastHeader(KAFKA_TEST_HEADER).value(),
        traceHeader.getBytes(StandardCharsets.UTF_8)))
      .withLimit(1)
      .build());
    context.assertEquals(1, keyValues.size());
    var actualHeaders = keyValues.get(0).getHeaders();
    OKAPI_HEADERS.forEach((key, value) ->
      context.assertTrue(Arrays.equals(value.getBytes(StandardCharsets.UTF_8), actualHeaders.lastHeader(key).value())));
    var actualOutgoingEvent = objectMapper.readValue(keyValues.get(0).getValue(), MarcBibUpdate.class);

    recordDao.getRecordByMatchedId(record.getMatchedId(), TENANT_ID).onComplete(getNew -> {
      if (getNew.failed()) {
        context.fail(getNew.cause());
      }
      context.assertTrue(getNew.result().isPresent());
      var updatedRecord = getNew.result().get();

      assertNewDatabaseRecord(context, updatedRecord, parsedRecord, updatedParsedRecordContent);
      assertOutgoingEvent(context, event, actualOutgoingEvent, updatedRecord, updateTargets);

      recordDao.getRecordById(record.getId(), TENANT_ID)
        .onComplete(assertOldDatabaseRecord(context, async, parsedRecord));
    });
  }

  @Test
  public void shouldUpdateMultipleRecordsAndSendMultipleRecordUpdatedEvents(TestContext context)
    throws InterruptedException {
    var updateTargets = buildUpdateTargets(instanceId, UUID.randomUUID().toString(), SECOND_INSTANCE_ID);
    var event = buildLinkEventForUpdate(updateTargets);

    var traceHeader = UUID.randomUUID().toString();
    cluster.send(createRequest(event, traceHeader));
    var values = readValuesFromKafka(traceHeader, 2);
    context.assertEquals(2, values.size());

    var eventsInstanceIds = values.stream()
      .map(value -> {
        try {
          return objectMapper.readValue(value, MarcBibUpdate.class).getRecord().getExternalIdsHolder().getInstanceId();
        } catch (IOException e) {
          return null;
        }})
      .filter(Objects::nonNull)
      .collect(Collectors.toList());

    context.assertTrue(List.of(instanceId, SECOND_INSTANCE_ID).containsAll(eventsInstanceIds));
  }

  /**
   * Only $9 subfield should be removed and only in case it matches authorityId from event
   * */
  @Test
  public void shouldUpdateBibRecordForDeleteEventAndSendRecordUpdatedEvent(TestContext context)
    throws InterruptedException, IOException {
    var async = context.async();

    var parsedRecord = record.getParsedRecord();
    var updateTargets = buildUpdateTargets();
    var event = buildLinkEvent(updateTargets, BibAuthorityLinksUpdate.Type.DELETE);

    var traceHeader = UUID.randomUUID().toString();
    cluster.send(createRequest(event, traceHeader));
    var values = readValuesFromKafka(traceHeader, 1);
    context.assertEquals(1, values.size());
    var actualOutgoingEvent = objectMapper.readValue(values.get(0), MarcBibUpdate.class);

    recordDao.getRecordByMatchedId(record.getMatchedId(), TENANT_ID).onComplete(getNew -> {
      if (getNew.failed()) {
        context.fail(getNew.cause());
      }
      context.assertTrue(getNew.result().isPresent());
      var updatedRecord = getNew.result().get();

      assertNewDatabaseRecord(context, updatedRecord, parsedRecord, unlinkedParsedRecordContent);
      assertOutgoingEvent(context, event, actualOutgoingEvent, updatedRecord, updateTargets);

      recordDao.getRecordById(record.getId(), TENANT_ID)
        .onComplete(assertOldDatabaseRecord(context, async, parsedRecord));
    });
  }

  private SendKeyValues<String, String> createRequest(Object payload, String traceValue) {
    var eventRecord = new KeyValue<>(KAFKA_KEY_NAME, Json.encode(payload));
    OKAPI_HEADERS.forEach((key, value) -> {
      eventRecord.addHeader(key, value, Charset.defaultCharset());
      eventRecord.addHeader(key, value, Charset.defaultCharset());
      eventRecord.addHeader(key, value, Charset.defaultCharset());
    });
    eventRecord.addHeader(KAFKA_TEST_HEADER, traceValue, Charset.defaultCharset());
    return SendKeyValues.to(KAFKA_CONSUMER_TOPIC, singletonList(eventRecord)).useDefaults();
  }

  private static String getTopicName(String name) {
    return formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, name);
  }

  private List<String> readValuesFromKafka(String traceHeader, int limit) throws InterruptedException {
    return cluster.readValues(ReadKeyValues.from(KAFKA_PRODUCER_TOPIC)
      .withMaxTotalPollTime(60, TimeUnit.SECONDS)
      .filterOnHeaders(headers -> Arrays.equals(headers.lastHeader(KAFKA_TEST_HEADER).value(),
        traceHeader.getBytes(StandardCharsets.UTF_8)))
      .withLimit(limit)
      .build());
  }

  private int getLinksCount(List<UpdateTarget> updateTargets) {
    return (int) updateTargets.stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream())
      .filter(link -> link.getInstanceId().equals(instanceId))
      .count();
  }

  private List<Link> buildLinks(String... instanceIds) {
    return Stream.of(instanceIds)
      .map(instanceId -> new Link()
        .withInstanceId(instanceId)
        .withLinkId(LINK_ID))
      .collect(Collectors.toList());
  }

  private BibAuthorityLinksUpdate buildLinkEvent(List<UpdateTarget> updateTargets, BibAuthorityLinksUpdate.Type type) {

    var subfieldChanges = List.of(
      new SubfieldsChange().withField("020")//repeatable, should update only one | $9 should be removed on DELETE
        .withSubfields(singletonList(new Subfield().withCode("a").withValue("2940447241 (electronic bk. updated)"))),
      new SubfieldsChange().withField("245")//should update/add subfields | $9 should be removed on DELETE
        .withSubfields(List.of(
          new Subfield().withCode("a").withValue("The fundamentals of typography updated"),//update
          new Subfield().withCode("b").withValue("new subfield"),//add new
          new Subfield().withCode("h").withValue(""))),//remove subfield on update (for empty subfields)
      new SubfieldsChange().withField("100")//controlled by different authority, should remain as is
        .withSubfields(singletonList(new Subfield().withCode("a").withValue("Ambrose, Gavin. updated"))),
      new SubfieldsChange().withField("123")//doesn't exist, should be ignored
        .withSubfields(singletonList(new Subfield().withCode("a").withValue("absent")))
    );

    return new BibAuthorityLinksUpdate()
      .withJobId(authorityJobId)
      .withAuthorityId(LINKED_AUTHORITY_ID)
      .withTenant(TENANT_ID)
      .withTs("123")
      .withType(type)
      .withUpdateTargets(updateTargets)
      .withSubfieldsChanges(subfieldChanges);
  }

  private List<UpdateTarget> buildUpdateTargets() {
    return buildUpdateTargets(instanceId, UUID.randomUUID().toString());
  }

  private List<UpdateTarget> buildUpdateTargets(String... instanceIds) {
    return List.of(
      new UpdateTarget().withField("020").withLinks(buildLinks(instanceIds)),
      new UpdateTarget().withField("245").withLinks(buildLinks(instanceIds)),
      new UpdateTarget().withField("100").withLinks(buildLinks(instanceIds)),
      new UpdateTarget().withField("222").withLinks(buildLinks(UUID.randomUUID().toString())),
      new UpdateTarget().withField("123").withLinks(buildLinks(instanceIds)));
  }

  private BibAuthorityLinksUpdate buildLinkEventForUpdate(List<UpdateTarget> updateTargets) {
    return buildLinkEvent(updateTargets, BibAuthorityLinksUpdate.Type.UPDATE);
  }

  private void assertOutgoingEvent(TestContext context, BibAuthorityLinksUpdate event, MarcBibUpdate actualOutgoingEvent,
                                   Record updatedRecord, List<UpdateTarget> updateTargets) {
    context.assertEquals(event.getJobId(), actualOutgoingEvent.getJobId());
    context.assertEquals(getLinksCount(updateTargets), actualOutgoingEvent.getLinkIds().size());
    context.assertEquals(LINK_ID, actualOutgoingEvent.getLinkIds().get(0));
    context.assertEquals(event.getTenant(), actualOutgoingEvent.getTenant());
    context.assertEquals(event.getTs(), actualOutgoingEvent.getTs());
    context.assertEquals(MarcBibUpdate.Type.UPDATE, actualOutgoingEvent.getType());
    context.assertEquals(updatedRecord.getId(), actualOutgoingEvent.getRecord().getId());
  }

  private Handler<AsyncResult<Optional<Record>>> assertOldDatabaseRecord(TestContext context, Async async,
                                                                         ParsedRecord parsedRecord) {
    return getOldFuture -> {
      if (getOldFuture.failed()) {
        context.fail(getOldFuture.cause());
      }

      context.assertTrue(getOldFuture.result().isPresent());
      var existingRecord = getOldFuture.result().get();

      context.assertEquals(State.OLD, existingRecord.getState());
      context.assertEquals(0, existingRecord.getGeneration());
      context.assertEquals(parsedRecord.getId(), existingRecord.getParsedRecord().getId());
      context.assertEquals(parsedRecord.getContent(), existingRecord.getParsedRecord().getContent());
      context.assertEquals(record.getSnapshotId(), existingRecord.getSnapshotId());

      async.complete();
    };
  }

  private void assertNewDatabaseRecord(TestContext context, Record updatedRecord,
                                       ParsedRecord parsedRecord, String expectedParsedRecordContent) {
    context.assertNotEquals(parsedRecord.getId(), updatedRecord.getParsedRecord().getId());
    context.assertNotEquals(record.getSnapshotId(), updatedRecord.getSnapshotId());
    context.assertEquals(record.getGeneration() + 1, updatedRecord.getGeneration());
    context.assertEquals(expectedParsedRecordContent,
      ParsedRecordDaoUtil.normalize(updatedRecord.getParsedRecord().getContent()).toString());
  }
}
