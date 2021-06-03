package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jooq.Tables;
import org.folio.rest.util.OkapiConnectionParams;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.folio.dao.util.QMEventTypes.QM_ERROR;
import static org.folio.dao.util.QMEventTypes.QM_RECORD_UPDATED;
import static org.folio.dao.util.QMEventTypes.QM_SRS_MARC_RECORD_UPDATED;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;

@RunWith(VertxUnitRunner.class)
public class QuickMarcKafkaHandlerTest extends AbstractLBServiceTest {

  private static final String UPDATED_PARSED_RECORD_CONTENT =
    "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  private static final String KAFKA_KEY_NAME = "test-key";
  private static final String recordId = UUID.randomUUID().toString();

  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private RecordDao recordDao;
  private RecordService recordService;
  private Record record;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
          .encode());
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);
    recordDao = new RecordDaoImpl(postgresClientFactory);
    recordService = new RecordServiceImpl(recordDao);
    Async async = context.async();
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordService.saveRecord(record, TENANT_ID))
      .onSuccess(ar -> async.complete())
      .onFailure(context::fail);
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
  public void shouldUpdateParsedRecordAndSendRecordUpdatedEvent(TestContext context) throws IOException, InterruptedException {
    Async async = context.async();

    ParsedRecord parsedRecord = record.getParsedRecord();

    Future<Record> future = recordService.saveRecord(record, TENANT_ID);

    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(record.getMatchedId())
      .withParsedRecord(new ParsedRecord()
        .withContent(UPDATED_PARSED_RECORD_CONTENT))
      .withRecordType(ParsedRecordDto.RecordType.MARC_BIB);

    var payload = new HashMap<String, String>();
    payload.put("PARSED_RECORD_DTO", Json.encode(parsedRecordDto));

    cluster.send(createRequest(payload));

    String observeTopic =
      formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QM_SRS_MARC_RECORD_UPDATED.name());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    future.onComplete(ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      }
      recordService.getSourceRecordById(record.getMatchedId(), ExternalIdType.RECORD, TENANT_ID).onComplete(getNew -> {
        if (getNew.failed()) {
          context.fail(getNew.cause());
        }
        context.assertTrue(getNew.result().isPresent());
        SourceRecord updatedRecord = getNew.result().get();

        context.assertNotEquals(parsedRecord.getId(), updatedRecord.getParsedRecord().getId());
        context.assertNotEquals(record.getSnapshotId(), updatedRecord.getSnapshotId());

        recordDao.getRecordByCondition(Tables.RECORDS_LB.ID.eq(UUID.fromString(record.getId())), TENANT_ID)
          .onComplete(getOld -> {
            if (getOld.failed()) {
              context.fail(getOld.cause());
            }
            context.assertTrue(getOld.result().isPresent());
            Record existingRecord = getOld.result().get();

            context.assertEquals(State.OLD, existingRecord.getState());
            context.assertEquals(0, existingRecord.getGeneration());
            context.assertEquals(parsedRecord.getId(), existingRecord.getParsedRecord().getId());
            context.assertEquals(parsedRecord.getContent(), existingRecord.getParsedRecord().getContent());
            context.assertEquals(record.getSnapshotId(), existingRecord.getSnapshotId());
            async.complete();
          });
      });
    });
  }

  @Test
  public void shouldSendErrorEventWhenNoDataInPayload() throws IOException, InterruptedException {
    cluster.send(createRequest(new HashMap<>()));

    String observeTopic = formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QM_ERROR.name());
    cluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());
  }

  private SendKeyValues<String, String> createRequest(HashMap<String, String> payload) throws IOException {
    String topic = formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QM_RECORD_UPDATED.name());
    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(ZIPArchiver.zip(Json.encode(payload)));
    KeyValue<String, String> eventRecord = new KeyValue<>(KAFKA_KEY_NAME, Json.encode(event));
    eventRecord.addHeader(OkapiConnectionParams.OKAPI_URL_HEADER, OKAPI_URL, Charset.defaultCharset());
    eventRecord.addHeader(OkapiConnectionParams.OKAPI_TENANT_HEADER, TENANT_ID, Charset.defaultCharset());
    eventRecord.addHeader(OkapiConnectionParams.OKAPI_TOKEN_HEADER, TOKEN, Charset.defaultCharset());
    return SendKeyValues.to(topic, Collections.singletonList(eventRecord)).useDefaults();
  }

}
