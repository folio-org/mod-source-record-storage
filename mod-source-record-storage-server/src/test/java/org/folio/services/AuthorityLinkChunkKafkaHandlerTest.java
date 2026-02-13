package org.folio.services;

import static java.util.Collections.singletonList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.EntityLinksKafkaTopic.INSTANCE_AUTHORITY;
import static org.folio.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.RecordStorageKafkaTopic.MARC_BIB;
import static org.folio.rest.jaxrs.model.LinkUpdateReport.Status.FAIL;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.kafka.services.KafkaTopic;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.BibAuthorityLinksUpdate;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Link;
import org.folio.rest.jaxrs.model.LinkUpdateReport;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Subfield;
import org.folio.rest.jaxrs.model.SubfieldsChange;
import org.folio.rest.jaxrs.model.UpdateTarget;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@RunWith(VertxUnitRunner.class)
public class AuthorityLinkChunkKafkaHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_MARC_RECORD_LINKED_PATH = "src/test/resources/parsedMarcRecordLinked.json";
  private static final String KAFKA_KEY_NAME = "test-key";
  private static final String KAFKA_TEST_HEADER = "x-okapi-test";
  private static final String KAFKA_CONSUMER_TOPIC = getTopicName(INSTANCE_AUTHORITY);
  private static final String KAFKA_SRS_BIB_PRODUCER_TOPIC = getTopicName(MARC_BIB);
  private static final String KAFKA_LINK_STATS_PRODUCER_TOPIC = getTopicName(LINKS_STATS);
  private static final String LINKED_AUTHORITY_ID = "6d19a8e8-2b71-482e-bfda-2b97f8722a2f";
  private static final String LINKED_BIB_UPDATE_JOB_ID = UUID.randomUUID().toString();
  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String INSTANCE_ID = UUID.randomUUID().toString();
  private static final String HR_ID = "testHRID";
  private static final String SECOND_RECORD_ID = UUID.randomUUID().toString();
  private static final String SECOND_INSTANCE_ID = UUID.randomUUID().toString();
  private static final String ERROR_RECORD_ID = UUID.randomUUID().toString();
  private static final String ERROR_INSTANCE_ID = UUID.randomUUID().toString();
  private static final String ERROR_HR_ID = "errorHRID";
  private static final String ERROR_RECORD_DESCRIPTION = "test error";
  private static final Integer LINK_ID = RandomUtils.nextInt();
  private static final String USER_ID = UUID.randomUUID().toString();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Map<String, String> OKAPI_HEADERS = Map.of(
    OKAPI_URL_HEADER, OKAPI_URL,
    OKAPI_TENANT_HEADER, TENANT_ID,
    OKAPI_TOKEN_HEADER, TOKEN,
    XOkapiHeaders.USER_ID, USER_ID
  );
  private final RawRecord rawRecord = new RawRecord().withId(RECORD_ID)
    .withContent("test content");
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;
  private RecordService recordService;
  private Record record;
  private Record secondRecord;
  private Record errorRecord;

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.openMocks(this);
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    recordService = new RecordServiceImpl(recordDao);

    var async = context.async();
    var snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    var content = new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_LINKED_PATH)).encode();
    var parsedRecord = new ParsedRecord().withId(RECORD_ID).withContent(content);
    record = new Record()
      .withId(RECORD_ID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withMatchedId(RECORD_ID)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(INSTANCE_ID).withInstanceHrid(HR_ID));

    var secondParsedRecord = new ParsedRecord().withId(SECOND_RECORD_ID)
      .withContent(new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_LINKED_PATH)).encode());
    secondRecord = new Record()
      .withId(SECOND_RECORD_ID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withMatchedId(SECOND_RECORD_ID)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(secondParsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_INSTANCE_ID).withInstanceHrid(HR_ID));

    var content2 = new JsonObject(TestUtil.readFileFromPath(PARSED_MARC_RECORD_LINKED_PATH)).encode();
    var errorRecordContent = new ErrorRecord().withId(ERROR_RECORD_ID).withContent(content2).withDescription(ERROR_RECORD_DESCRIPTION);
    var errorParsedRecord = new ParsedRecord().withContent(ERROR_RECORD_ID).withContent(content2);
    errorRecord = new Record()
      .withId(ERROR_RECORD_ID)
      .withRawRecord(rawRecord)
      .withMatchedId(ERROR_RECORD_ID)
      .withErrorRecord(errorRecordContent)
      .withParsedRecord(errorParsedRecord)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(ERROR_INSTANCE_ID).withInstanceHrid(ERROR_HR_ID));

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordService.saveRecord(record, okapiHeaders))
      .compose(savedRecord -> recordService.saveRecord(secondRecord, okapiHeaders))
      .compose(savedRecord -> recordService.saveRecord(errorRecord, okapiHeaders))
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
  @SneakyThrows
  public void shouldUpdateBibRecordAndSendRecordUpdatedEvent(TestContext context) {
    var async = context.async();

    var parsedRecord = record.getParsedRecord();
    var updateTargets = buildUpdateTargets();
    var event = buildLinkEventForUpdate(updateTargets);

    var traceHeader = UUID.randomUUID().toString();
    send(event, traceHeader);

    var keyValues = readConsumerRecordsFromKafka(KAFKA_SRS_BIB_PRODUCER_TOPIC, traceHeader, 1);
    context.assertEquals(1, keyValues.size());
    var actualHeaders = keyValues.getFirst().headers();
    OKAPI_HEADERS.forEach((key, value) ->
      context.assertEquals(value, new String(actualHeaders.lastHeader(key).value(), UTF_8)));
    var actualOutgoingEvent = objectMapper.readValue(keyValues.getFirst().value(), MarcBibUpdate.class);

    recordDao.getRecordByMatchedId(record.getMatchedId(), TENANT_ID).onComplete(getNew -> {
      if (getNew.failed()) {
        context.fail(getNew.cause());
      }
      context.assertTrue(getNew.result().isPresent());
      var updatedRecord = getNew.result().get();

      assertNewDatabaseRecord(context, updatedRecord, parsedRecord);
      assertOutgoingEvent(context, event, actualOutgoingEvent, updatedRecord, updateTargets);

      recordDao.getRecordById(record.getId(), TENANT_ID)
        .onComplete(assertOldDatabaseRecord(context, async, parsedRecord));
    });
  }

  @Test
  public void shouldUpdateMultipleRecordsAndSendMultipleRecordUpdatedEvents(TestContext context) {
    var updateTargets = buildUpdateTargets(INSTANCE_ID, UUID.randomUUID().toString(), SECOND_INSTANCE_ID);
    var event = buildLinkEventForUpdate(updateTargets);

    var traceHeader = UUID.randomUUID().toString();
    send(event, traceHeader);
    var values = readValuesFromKafka(KAFKA_SRS_BIB_PRODUCER_TOPIC, traceHeader, 2);
    context.assertEquals(2, values.size());

    var eventsInstanceIds = values.stream()
      .map(value -> {
        try {
          return objectMapper.readValue(value, MarcBibUpdate.class).getRecord().getExternalIdsHolder().getInstanceId();
        } catch (IOException e) {
          return null;
        }
      })
      .filter(Objects::nonNull)
      .toList();

    context.assertTrue(List.of(INSTANCE_ID, SECOND_INSTANCE_ID).containsAll(eventsInstanceIds));
  }

  /**
   * Only $9 subfield should be removed and only in case it matches authorityId from event
   */
  @Test
  @SneakyThrows
  public void shouldUpdateBibRecordForDeleteEventAndSendRecordUpdatedEvent(TestContext context) {
    var async = context.async();

    var parsedRecord = record.getParsedRecord();
    var updateTargets = buildUpdateTargets();
    var event = buildLinkEvent(updateTargets, BibAuthorityLinksUpdate.Type.DELETE);

    var traceHeader = UUID.randomUUID().toString();
    send(event, traceHeader);
    var values = readValuesFromKafka(KAFKA_SRS_BIB_PRODUCER_TOPIC, traceHeader, 1);
    context.assertEquals(1, values.size());
    var actualOutgoingEvent = objectMapper.readValue(values.getFirst(), MarcBibUpdate.class);

    recordDao.getRecordByMatchedId(record.getMatchedId(), TENANT_ID).onComplete(getNew -> {
      if (getNew.failed()) {
        context.fail(getNew.cause());
      }
      context.assertTrue(getNew.result().isPresent());
      var updatedRecord = getNew.result().get();

      assertNewDatabaseRecord(context, updatedRecord, parsedRecord);
      assertOutgoingEvent(context, event, actualOutgoingEvent, updatedRecord, updateTargets);

      recordDao.getRecordById(record.getId(), TENANT_ID)
        .onComplete(assertOldDatabaseRecord(context, async, parsedRecord));
    });
  }

  @Test
  @SneakyThrows
  public void shouldUpdateMultipleRecordsAndSendOneFailedLinkUpdateReport(TestContext context) {
    var event = buildLinkEventForUpdate(buildUpdateTargets(INSTANCE_ID, SECOND_INSTANCE_ID, ERROR_INSTANCE_ID));
    var traceHeader = UUID.randomUUID().toString();

    send(event, traceHeader);
    var values = readValuesFromKafka(KAFKA_LINK_STATS_PRODUCER_TOPIC, traceHeader, 1);
    context.assertEquals(1, values.size());

    var report = objectMapper.readValue(values.getFirst(), LinkUpdateReport.class);
    context.assertEquals(FAIL, report.getStatus());
    context.assertEquals(ERROR_INSTANCE_ID, report.getInstanceId());
    context.assertEquals(ERROR_RECORD_DESCRIPTION, report.getFailCause());
  }

  private void send(Object payload, String traceValue) {
    Map<String, String> headers = new HashMap<>(OKAPI_HEADERS);
    headers.put(KAFKA_TEST_HEADER, traceValue);
    send(KAFKA_CONSUMER_TOPIC, KAFKA_KEY_NAME, Json.encode(payload), headers);
  }

  private static String getTopicName(KafkaTopic topic) {
    return topic.fullTopicName(kafkaConfig, TENANT_ID);
  }

  private List<String> readValuesFromKafka(String topic, String traceHeader, int limit) {
    return readConsumerRecordsFromKafka(topic, traceHeader, limit)
        .stream()
        .map(ConsumerRecord::value)
        .toList();
  }

  private List<ConsumerRecord<String, String>> readConsumerRecordsFromKafka(String topic, String traceHeader, int limit) {
    List<ConsumerRecord<String, String>> list = new ArrayList<>();
    while (true) {
      var list2 = getKafkaEvents(topic);
      if (list2.isEmpty()) {
        break;
      }
      list2.removeIf(consumerRecord -> !hasTraceHeader(consumerRecord, traceHeader));
      list.addAll(list2);
      if (list.size() >= limit) {
        break;
      }
    }
    return list;
  }

  private boolean hasTraceHeader(ConsumerRecord<String, String> consumerRecord, String traceHeader) {
    var header = consumerRecord.headers().lastHeader(KAFKA_TEST_HEADER);
    return (header != null) && traceHeader.equals(new String(header.value(), UTF_8));
  }

  private int getLinksCount(List<UpdateTarget> updateTargets) {
    return (int) updateTargets.stream()
      .flatMap(updateTarget -> updateTarget.getLinks().stream())
      .filter(link -> link.getInstanceId().equals(INSTANCE_ID))
      .count();
  }

  private List<Link> buildLinks(String... instanceIds) {
    return Stream.of(instanceIds)
      .map(instanceId -> new Link()
        .withInstanceId(instanceId)
        .withLinkId(LINK_ID))
      .toList();
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
      .withJobId(LINKED_BIB_UPDATE_JOB_ID)
      .withAuthorityId(LINKED_AUTHORITY_ID)
      .withTenant(TENANT_ID)
      .withTs("123")
      .withType(type)
      .withUpdateTargets(updateTargets)
      .withSubfieldsChanges(subfieldChanges);
  }

  private List<UpdateTarget> buildUpdateTargets() {
    return buildUpdateTargets(INSTANCE_ID, UUID.randomUUID().toString());
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
    context.assertEquals(LINK_ID, actualOutgoingEvent.getLinkIds().getFirst());
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
                                       ParsedRecord parsedRecord) {
    context.assertNotEquals(parsedRecord.getId(), updatedRecord.getParsedRecord().getId());
    context.assertNotEquals(record.getSnapshotId(), updatedRecord.getSnapshotId());
    context.assertEquals(record.getGeneration() + 1, updatedRecord.getGeneration());
    context.assertEquals(USER_ID, updatedRecord.getMetadata().getUpdatedByUserId());
  }
}
