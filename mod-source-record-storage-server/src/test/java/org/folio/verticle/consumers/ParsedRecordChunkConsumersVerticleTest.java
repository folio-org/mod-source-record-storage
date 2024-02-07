package org.folio.verticle.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.consumers.ParsedRecordChunksKafkaHandler.JOB_EXECUTION_ID_HEADER;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordChunkConsumersVerticleTest extends AbstractLBServiceTest {

  private static final String KAFKA_KEY_NAME = "test-key";
  public static final String WRONG_LEADER_STATUS = "wrong leader status";
  public static final int EXPECTED_ERROR_EVENTS_NUMBER = 2;

  private static String recordId = UUID.randomUUID().toString();

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
  public void shouldSendEventWithSavedMarcBibRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException {
    sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType.MARC_BIB, rawMarcRecord,
      parsedMarcRecord);
  }

  @Test
  public void shouldSendEventWithSavedMarcAuthorityRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException {
    sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType.MARC_AUTHORITY,
      rawMarcRecord,
      parsedMarcRecord);
  }

  @Test
  public void shouldSendEventWithSavedEdifactRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException {
    sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType.EDIFACT, rawEdifactRecord,
      parsedEdifactRecord);
  }

  private void sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType recordType,
    RawRecord rawRecord, ParsedRecord parsedRecord) throws InterruptedException {
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

    String topic = KafkaTopicNameHelper
      .formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_RAW_RECORDS_CHUNK_PARSED.value());
    Event event = new Event().withEventPayload(Json.encode(recordCollection));
    KeyValue<String, String> record = new KeyValue<>(KAFKA_KEY_NAME, Json.encode(event));
    record.addHeader(OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN, Charset.defaultCharset());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(record)).useDefaults();

    cluster.send(request);

    String observeTopic = KafkaTopicNameHelper
      .formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_PARSED_RECORDS_CHUNK_SAVED.value());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());
  }

  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordChunkWasNotSaved() throws InterruptedException {
    Record validRecord = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record additionalRecord = getAdditionalRecord(validRecord, snapshotId, validRecord.getRecordType());
    List<Record> records = List.of(validRecord, additionalRecord);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "ERROR: insert or update on table \"raw_records_lb\" violates foreign key constraint \"fk_raw_records_records\"" );
  }

  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordsHaveDifferentSnapshotIds() throws InterruptedException {
    Record first = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record secondWithDifferentSnapshotId = getAdditionalRecord(first, UUID.randomUUID().toString(), first.getRecordType());
    List<Record> records = List.of(first, secondWithDifferentSnapshotId);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "Batch record collection only supports single snapshot" );
  }

  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordsHaveDifferentRecordTypes() throws InterruptedException {
    Record first = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record secondWithDifferentRecordType = getAdditionalRecord(first, snapshotId, RecordType.MARC_AUTHORITY);
    List<Record> records = List.of(first, secondWithDifferentRecordType);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "Batch record collection only supports single record type" );
  }

  @Test
  public void shouldSendDIErrorEventsWhenSnapshotsNotFound() throws InterruptedException {
    String snapshotId = UUID.randomUUID().toString();
    Record first = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    Record second = TestMocks.getRecord(0).withSnapshotId(snapshotId);
    List<Record> records = List.of(first, second);
    String jobExecutionId = UUID.randomUUID().toString();

    sendRecordsToKafka(jobExecutionId, records);

    check_DI_ERROR_eventsSent(jobExecutionId, records, "Snapshot with id", "was not found" );
  }

  @Test
  public void shouldNotSendDIErrorWhenReceivedDuplicateChunksParsedEvent() throws InterruptedException {
    sendDuplicateEventAndObserveRecords(DI_ERROR.value(), 0);
  }

  @Test
  public void shouldNotSendDuplicateChunksSavedEventWhenReceivedDuplicateChunksParsedEvent() throws InterruptedException {
    sendDuplicateEventAndObserveRecords(DI_PARSED_RECORDS_CHUNK_SAVED.value(), 1);
  }

  private void sendDuplicateEventAndObserveRecords(String eventTypeValue, int expectedCount) throws InterruptedException {
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
    List<String> observedValues = cluster.observeValues(ObserveKeyValues.on(observeTopic, expectedCount)
      .observeFor(30, TimeUnit.SECONDS)
      .build());
  }

  private void sendRecordsToKafka(String jobExecutionId, List<Record> records) throws InterruptedException {
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());

    String topic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_RAW_RECORDS_CHUNK_PARSED.value());
    Event event = new Event().withEventPayload(Json.encode(recordCollection));
    KeyValue<String, String> record = new KeyValue<>(KAFKA_KEY_NAME, Json.encode(event));
    record.addHeader(OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN, Charset.defaultCharset());
    record.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionId, Charset.defaultCharset());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(record)).useDefaults();

    cluster.send(request);
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

  private void check_DI_ERROR_eventsSent(String jobExecutionId, List<Record> records, String... errorMessages) throws InterruptedException {
    List<DataImportEventPayload> testedEventsPayLoads = new ArrayList<>();
    String observeTopic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_ERROR.value());
    List<String> observedValues = cluster.readValues(ReadKeyValues.from(observeTopic).build());
    if (CollectionUtils.isEmpty(observedValues)) {
      observedValues = cluster.observeValues(ObserveKeyValues.on(observeTopic, records.size())
        .observeFor(30, TimeUnit.SECONDS)
        .build());
    }
    for (String observedValue : observedValues) {
      Event obtainedEvent = Json.decodeValue(observedValue, Event.class);
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
      assertEquals(DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value(), eventPayload.getEventsChain().get(0));
    }
  }
}
