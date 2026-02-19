package org.folio.verticle.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.errorhandlers.ParsedRecordChunksErrorHandler;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.AbstractLBServiceTest;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.UUID;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.consumers.ParsedRecordChunksKafkaHandler.JOB_EXECUTION_ID_HEADER;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordChunkConsumersVerticleTest extends AbstractLBServiceTest {

  private static final String KAFKA_KEY_NAME = "test-key";
  public static final String WRONG_LEADER_STATUS = "wrong leader status";
  public static final int EXPECTED_ERROR_EVENTS_NUMBER = 2;

  private static String recordId = UUID.randomUUID().toString();

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  private static RawRecord rawMarcRecord;
  private static ParsedRecord parsedMarcRecord;

  private static RawRecord rawEdifactRecord;
  private static ParsedRecord parsedEdifactRecord;

  private String snapshotId = UUID.randomUUID().toString();

  @BeforeClass
  public static void loadMockRecords(TestContext context) throws IOException {
    rawMarcRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedMarcRecord = new ParsedRecord()
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
    rawEdifactRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedEdifactRecord = new ParsedRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);
    Async async = context.async();

    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(snapshotId)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldSendEventWithSavedMarcBibRecordCollectionPayloadAfterProcessingParsedRecordEvent() {
    assertSentEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(
        RecordType.MARC_BIB, rawMarcRecord, parsedMarcRecord);
  }

  @Test
  public void shouldSendEventWithSavedMarcAuthorityRecordCollectionPayloadAfterProcessingParsedRecordEvent() {
    assertSentEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(
        RecordType.MARC_AUTHORITY, rawMarcRecord, parsedMarcRecord);
  }

  @Test
  public void shouldSendEventWithSavedEdifactRecordCollectionPayloadAfterProcessingParsedRecordEvent() {
    assertSentEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(
        RecordType.EDIFACT, rawEdifactRecord, parsedEdifactRecord);
  }

  private void assertSentEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(
      RecordType recordType, RawRecord rawRecord, ParsedRecord parsedRecord) {

    List<Record> records = new ArrayList<>();

    records.add(new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId)
      .withGeneration(0)
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord));

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());

    Event event = new Event().withEventPayload(Json.encode(recordCollection));
    var jobExecutionId = UUID.randomUUID().toString();
    send(Json.encode(event), jobExecutionId);

    String observeTopic = KafkaTopicNameHelper
      .formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_PARSED_RECORDS_CHUNK_SAVED.value());
    var saveEvent = getKafkaEvent(observeTopic);
    assertNotNull(saveEvent);
  }

  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordChunkWasNotSaved() {
    Record validRecord = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record additionalRecord = getAdditionalRecord(validRecord, snapshotId, validRecord.getRecordType());
    List<Record> records = List.of(validRecord, additionalRecord);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records,
      "ERROR: insert or update on table \"raw_records_lb\" violates foreign key constraint \"fk_raw_records_records\"");
  }

  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordsHaveDifferentSnapshotIds() {
    Record first = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record secondWithDifferentSnapshotId = getAdditionalRecord(first, UUID.randomUUID().toString(), first.getRecordType());
    List<Record> records = List.of(first, secondWithDifferentSnapshotId);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "Batch record collection only supports single snapshot" );
  }

  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordsHaveDifferentRecordTypes() {
    Record first = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record secondWithDifferentRecordType = getAdditionalRecord(first, snapshotId, RecordType.MARC_AUTHORITY);
    List<Record> records = List.of(first, secondWithDifferentRecordType);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "Batch record collection only supports single record type" );
  }

  @Test
  public void shouldSendDIErrorEventsWhenSnapshotsNotFound() {
    String snapshotId = UUID.randomUUID().toString();
    Record first = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record second = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    List<Record> records = List.of(first, second);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "Snapshot with id", "was not found" );
  }

  @Test
  public void shouldNotSendDIErrorWhenReceivedDuplicateChunksParsedEvent() {
    check_sendDuplicateEventAndObserveRecords(DI_ERROR.value(), 0);
  }

  @Test
  public void shouldNotSendDuplicateChunksSavedEventWhenReceivedDuplicateChunksParsedEvent() {
    check_sendDuplicateEventAndObserveRecords(DI_PARSED_RECORDS_CHUNK_SAVED.value(), 1);
  }

  private void check_sendDuplicateEventAndObserveRecords(String eventTypeValue, int expectedCount) {
    String jobExecutionId = UUID.randomUUID().toString();
    List<Record> records = new ArrayList<>();

    records.add(new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId)
      .withGeneration(0)
      .withRecordType(RecordType.MARC_AUTHORITY)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord));

    sendRecordsToKafka(jobExecutionId, records);
    sendRecordsToKafka(jobExecutionId, records);

    String observeTopic = KafkaTopicNameHelper
        .formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, eventTypeValue);
    var events = getKafkaEvents(observeTopic);
    assertThat(events, hasSize(expectedCount));
  }

  private void sendRecordsToKafka(String jobExecutionId, List<Record> records) {
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    Event event = new Event().withEventPayload(Json.encode(recordCollection));
    send(Json.encode(event), jobExecutionId);
  }

  private void send(String value, String jobExecutionId) {
    String topic = KafkaTopicNameHelper.formatTopicName(
        kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_RAW_RECORDS_CHUNK_PARSED.value());
    var headers = new HashMap<String, String>();
    headers.putAll(Map.of(
        OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL,
        OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID,
        OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN,
        JOB_EXECUTION_ID_HEADER, jobExecutionId
        ));
    send(topic, KAFKA_KEY_NAME, value, headers);
  }

  private Record getAdditionalRecord(Record validRecord, String snapshotId, RecordType recordType) {
    return new Record()
      .withId(UUID.randomUUID().toString())
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(snapshotId)
      .withRecordType(recordType)
      .withRawRecord(validRecord.getRawRecord())
      .withParsedRecord(validRecord.getParsedRecord())
      .withLeaderRecordStatus(WRONG_LEADER_STATUS);
  }

  private void check_DI_ERROR_eventsSent(String jobExecutionId, List<Record> records, String... errorMessages) {
    List<DataImportEventPayload> testedEventsPayLoads = new ArrayList<>();
    String observeTopic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_ERROR.value());
    var observedEvents = new ArrayList<ConsumerRecord<String, String>>();
    while (observedEvents.size() < records.size()) {
      var list = getKafkaEvents(observeTopic);
      if (list.isEmpty()) {
        break;
      }
      observedEvents.addAll(list);
    }
    for (var observedEvent : observedEvents) {
      Event obtainedEvent = Json.decodeValue(observedEvent.value(), Event.class);
      DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
      if (jobExecutionId.equals(eventPayload.getJobExecutionId())) {
        testedEventsPayLoads.add(eventPayload);
      }
    }

    assertEquals(EXPECTED_ERROR_EVENTS_NUMBER, testedEventsPayLoads.size());

    for (DataImportEventPayload eventPayload : testedEventsPayLoads) {
      String recordId = eventPayload.getContext().get(ParsedRecordChunksErrorHandler.RECORD_ID_HEADER);
      String error = eventPayload.getContext().get(ParsedRecordChunksErrorHandler.ERROR_KEY);
      assertEquals(DI_ERROR.value(), eventPayload.getEventType());
      assertEquals(TENANT_ID, eventPayload.getTenant());
      assertTrue(StringUtils.isNotBlank(recordId));
      for (String errorMessage: errorMessages) {
        assertTrue(error.contains(errorMessage));
      }
      assertFalse(eventPayload.getEventsChain().isEmpty());
      assertEquals(DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value(), eventPayload.getEventsChain().getFirst());
    }
  }
}
