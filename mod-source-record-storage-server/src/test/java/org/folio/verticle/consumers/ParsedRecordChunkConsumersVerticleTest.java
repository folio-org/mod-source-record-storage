package org.folio.verticle.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.services.ParsedRecordChunksKafkaHandler.JOB_EXECUTION_ID_HEADER;
import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordChunkConsumersVerticleTest extends AbstractLBServiceTest {

  private static final String KAFKA_KEY_NAME = "test-key";

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
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
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
  public void shouldSendEventWithSavedMarcBibRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException, IOException {
    sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType.MARC_BIB, rawMarcRecord,
      parsedMarcRecord);
  }

  @Test
  public void shouldSendEventWithSavedMarcAuthorityRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException, IOException {
    sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType.MARC_AUTHORITY,
      rawMarcRecord,
      parsedMarcRecord);
  }

  @Test
  public void shouldSendEventWithSavedEdifactRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException, IOException {
    sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType.EDIFACT, rawEdifactRecord,
      parsedEdifactRecord);
  }

  private void sendEventWithSavedMarcRecordCollectionPayloadAfterProcessingParsedRecordEvent(RecordType recordType,
    RawRecord rawRecord, ParsedRecord parsedRecord) throws IOException, InterruptedException {
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
    Event event = new Event().withEventPayload(ZIPArchiver.zip(Json.encode(recordCollection)));
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

  @Ignore
  @Test
  public void shouldSendDIErrorEventsWhenParsedRecordChunkWasNotSaved() throws InterruptedException, IOException {
    Record validRecord = TestMocks.getRecord(0).withSnapshotId(snapshotId);

    Record invalidRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(snapshotId)
      .withRecordType(validRecord.getRecordType())
      .withRawRecord(validRecord.getRawRecord())
      .withParsedRecord(validRecord.getParsedRecord())
      // set invalid status to cause records saving error
      .withLeaderRecordStatus("wrong leader status");

    List<Record> records = List.of(validRecord, invalidRecord);
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());

    String topic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_RAW_RECORDS_CHUNK_PARSED.value());
    Event event = new Event().withEventPayload(ZIPArchiver.zip(Json.encode(recordCollection)));
    KeyValue<String, String> record = new KeyValue<>(KAFKA_KEY_NAME, Json.encode(event));
    record.addHeader(OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN, Charset.defaultCharset());
    record.addHeader(JOB_EXECUTION_ID_HEADER, snapshotId, Charset.defaultCharset());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(record)).useDefaults();

    cluster.send(request);

    String observeTopic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_ERROR.value());
    List<String> observedValues = cluster.observeValues(ObserveKeyValues.on(observeTopic, records.size())
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    assertEquals(2, observedValues.size());

    for (String observedValue : observedValues) {
      Event obtainedEvent = Json.decodeValue(observedValue, Event.class);
      assertEquals(DI_ERROR.value(), obtainedEvent.getEventType());

      DataImportEventPayload eventPayload = Json.decodeValue(ZIPArchiver.unzip(obtainedEvent.getEventPayload()), DataImportEventPayload.class);
      assertEquals(snapshotId, eventPayload.getJobExecutionId());
      assertEquals(DI_ERROR.value(), eventPayload.getEventType());
    }

  }

}
