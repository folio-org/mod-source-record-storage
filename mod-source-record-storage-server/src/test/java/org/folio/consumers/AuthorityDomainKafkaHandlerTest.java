package org.folio.consumers;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.IdType;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.okapi.common.XOkapiHeaders;
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
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AuthorityDomainKafkaHandlerTest extends AbstractLBServiceTest {

  private static final String RECORD_ID = UUID.randomUUID().toString();
  private static final String CURRENT_DATE = "20240718132044.6";
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  @Mock
  private ConsortiumConfigurationCache consortiumConfigurationCache;
  private RecordDao recordDao;
  private RecordService recordService;
  private Record record;
  private AuthorityDomainKafkaHandler handler;

  @BeforeAll
  static void setUpClassAuthority() throws IOException {
    rawRecord = new RawRecord().withId(RECORD_ID)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(RECORD_ID)
      .withContent(
        new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray()
          .add(new JsonObject().put("005", CURRENT_DATE))));
  }

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    recordService = new RecordServiceImpl(recordDao, consortiumConfigurationCache);
    handler = new AuthorityDomainKafkaHandler(recordService);
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    record = new Record()
      .withId(RECORD_ID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(RECORD_ID)
      .withExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(RECORD_ID))
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordService.saveRecord(record, okapiHeaders))
      .onComplete(testContext.succeedingThenComplete());
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldSoftDeleteMarcAuthorityRecordOnSoftDeleteDomainEvent(VertxTestContext testContext) {
    var payload = new HashMap<String, String>();
    payload.put("deleteEventSubType", "SOFT_DELETE");
    payload.put("tenant", TENANT_ID);

    handler.handle(new KafkaConsumerRecordImpl<>(getConsumerRecord(payload)))
      .compose(ar -> recordService.getSourceRecordById(record.getId(), IdType.RECORD, RecordState.DELETED, TENANT_ID))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertTrue(result.isPresent());
        SourceRecord updatedRecord = result.get();
        assertTrue(updatedRecord.getDeleted());
        assertTrue(updatedRecord.getAdditionalInfo().getSuppressDiscovery());
        assertEquals("d", ParsedRecordDaoUtil.getLeaderStatus(updatedRecord.getParsedRecord()));

        LinkedHashMap<String, ArrayList<LinkedHashMap<String, String>>> content =
          (LinkedHashMap<String, ArrayList<LinkedHashMap<String, String>>>) updatedRecord.getParsedRecord().getContent();
        LinkedHashMap<String, String> map = content.get("fields").getFirst();
        String resulted005FieldValue = map.get("005");
        assertNotNull(resulted005FieldValue);
        assertNotEquals(CURRENT_DATE, resulted005FieldValue);
        testContext.completeNow();
      })));
  }

  @Test
  void shouldHardDeleteMarcAuthorityRecordOnHardDeleteDomainEvent(VertxTestContext testContext) {
    var payload = new HashMap<String, String>();
    payload.put("deleteEventSubType", "HARD_DELETE");
    payload.put("tenant", TENANT_ID);

    handler.handle(new KafkaConsumerRecordImpl<>(getConsumerRecord(payload)))
      .compose(ar -> recordService.getSourceRecordById(record.getId(), IdType.RECORD, RecordState.ACTUAL, TENANT_ID))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertFalse(result.isPresent());
        testContext.completeNow();
      })));
  }

  @NotNull
  private ConsumerRecord<String, String> getConsumerRecord(HashMap<String, String> payload) {
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("topic", 1, 1, RECORD_ID, Json.encode(payload));
    consumerRecord.headers().add(new RecordHeader("domain-event-type", "DELETE".getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(XOkapiHeaders.URL, OKAPI_URL.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(XOkapiHeaders.TENANT, TENANT_ID.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(XOkapiHeaders.TOKEN, TOKEN.getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }

}
