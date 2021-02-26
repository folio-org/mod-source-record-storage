package org.folio.verticle.consumers;

import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.TestUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
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

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordChunkConsumersVerticleTest extends AbstractLBServiceTest {

  private static final String KAFKA_KEY_NAME = "test-key";

  private static String recordId = UUID.randomUUID().toString();

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private String snapshotId1 = UUID.randomUUID().toString();
  private String snapshotId2 = UUID.randomUUID().toString();

  @BeforeClass
  public static void loadMockRecords(TestContext context) throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);
    Async async = context.async();

    Snapshot snapshot1 = new Snapshot()
      .withJobExecutionId(snapshotId1)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    Snapshot snapshot2 = new Snapshot()
      .withJobExecutionId(snapshotId2)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    List<Snapshot> snapshots = new ArrayList<>();
    snapshots.add(snapshot1);
    snapshots.add(snapshot2);

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshots).onComplete(save -> {
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
  public void shouldSendEventWithSavedRecordCollectionPayloadAfterProcessingParsedRecordEvent(TestContext context) throws InterruptedException, IOException {
    Async async = context.async();

    List<Record> records = new ArrayList<>();

    records.add(new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord));

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());

    String topic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_RAW_RECORDS_CHUNK_PARSED.value());
    Event event = new Event().withEventPayload(ZIPArchiver.zip(Json.encode(recordCollection)));
    KeyValue<String, String> record = new KeyValue<>(KAFKA_KEY_NAME, Json.encode(event));
    record.addHeader(OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID, Charset.defaultCharset());
    record.addHeader(OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN, Charset.defaultCharset());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(record)).useDefaults();

    // when
    cluster.send(request);

    // then
    String observeTopic = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, DI_PARSED_RECORDS_CHUNK_SAVED.value());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    RecordDaoUtil.findById(postgresClientFactory.getQueryExecutor(TENANT_ID), recordId).onComplete(ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      }
      context.assertTrue(ar.result().isPresent());
      async.complete();
    });
  }

}
