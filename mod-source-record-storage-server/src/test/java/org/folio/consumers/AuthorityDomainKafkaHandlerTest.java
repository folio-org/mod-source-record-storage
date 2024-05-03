package org.folio.consumers;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.IdType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AuthorityDomainKafkaHandlerTest extends AbstractLBServiceTest {

  private static final String recordId = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private RecordDao recordDao;
  private RecordService recordService;
  private Record record;
  private AuthorityDomainKafkaHandler handler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
  }

  @Before
  public void setUp(TestContext context) {
    recordDao = new RecordDaoImpl(postgresClientFactory);
    recordService = new RecordServiceImpl(recordDao);
    handler = new AuthorityDomainKafkaHandler(recordService);
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
      .withExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(recordId))
      .withRecordType(MARC_AUTHORITY)
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
  public void shouldSoftDeleteMarcAuthorityRecordOnSoftDeleteDomainEvent(TestContext context) {
    Async async = context.async();

    var payload = new HashMap<String, String>();
    payload.put("deleteEventSubType", "SOFT_DELETE");
    payload.put("tenant", TENANT_ID);

    handler.handle(new KafkaConsumerRecordImpl<>(getConsumerRecord(payload)))
      .onComplete(ar -> {
        if (ar.failed()) {
          context.fail(ar.cause());
        }
        recordService.getSourceRecordById(record.getId(), IdType.RECORD, RecordState.DELETED, TENANT_ID)
          .onComplete(result -> {
            if (result.failed()) {
              context.fail(result.cause());
            }
            context.assertTrue(result.result().isPresent());
            SourceRecord updatedRecord = result.result().get();
            context.assertTrue(updatedRecord.getDeleted());
            async.complete();
          });
      });
  }

  @Test
  public void shouldHardDeleteMarcAuthorityRecordOnHardDeleteDomainEvent(TestContext context) {
    Async async = context.async();

    var payload = new HashMap<String, String>();
    payload.put("deleteEventSubType", "HARD_DELETE");
    payload.put("tenant", TENANT_ID);

    handler.handle(new KafkaConsumerRecordImpl<>(getConsumerRecord(payload)))
      .onComplete(ar -> {
        if (ar.failed()) {
          context.fail(ar.cause());
        }
        recordService.getSourceRecordById(record.getId(), IdType.RECORD, RecordState.ACTUAL, TENANT_ID)
          .onComplete(result -> {
            if (result.failed()) {
              context.fail(result.cause());
            }
            context.assertFalse(result.result().isPresent());
            async.complete();
          });
      });
  }

  @NotNull
  private ConsumerRecord<String, String> getConsumerRecord(HashMap<String, String> payload) {
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, recordId, Json.encode(payload));
    consumerRecord.headers().add(new RecordHeader("domain-event-type", "DELETE".getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }

}
