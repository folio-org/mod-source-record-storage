package org.folio.services;

import static java.util.Comparator.comparing;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.services.RecordServiceImpl.INDICATOR;
import static org.folio.services.RecordServiceImpl.SUBFIELD_S;
import static org.folio.services.RecordServiceImpl.UPDATE_RECORD_DUPLICATE_EXCEPTION;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.AdditionalFieldsUtil.getFieldFromMarcRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Flowable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.IdType;
import org.folio.dao.util.MarcUtil;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.Conditions;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.FetchParsedRecordsBatchRequest;
import org.folio.rest.jaxrs.model.FieldRange;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.StrippedParsedRecord;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RecordServiceTest extends AbstractLBServiceTest {

  private static final String MARC_BIB_RECORD_SNAPSHOT_ID = "d787a937-cc4b-49b3-85ef-35bcd643c689";
  private static final String MARC_AUTHORITY_RECORD_SNAPSHOT_ID = "ee561342-3098-47a8-ab6e-0f3eba120b04";
  private static final String HR_ID = "inst00007";

  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  @Mock
  private ConsortiumConfigurationCache consortiumConfigurationCache;

  private RecordDao recordDao;

  private RecordService recordService;

  private static RawRecord rawRecord;
  private static ParsedRecord marcRecord;

  @BeforeEach
  void setUp(VertxTestContext testContext) throws IOException {
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    marcRecord = new ParsedRecord()
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    recordService = new RecordServiceImpl(recordDao, consortiumConfigurationCache);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots())
      .onComplete(testContext.succeedingThenComplete());
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldGetMarcBibRecordsBySnapshotId(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      assertTrue(batch.succeeded());
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.MARC_BIB, orderFields, 1, 2, TENANT_ID).onComplete(get -> {
        assertTrue(get.succeeded());
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .sorted(comparing(Record::getOrder))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(expected.get(1), get.result().getRecords().get(0));
        compareRecords(expected.get(2), get.result().getRecords().get(1));
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFetchBibRecordsWithFieldsRangeByExternalId(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      assertTrue(batch.succeeded());
      String externalId = "3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc";
      List<FieldRange> data = List.of(new FieldRange().withFrom("001").withTo("999"));

      Conditions conditions = new Conditions()
        .withIdType(IdType.INSTANCE.name())
        .withIds(List.of(externalId));
      FetchParsedRecordsBatchRequest batchRequest = new FetchParsedRecordsBatchRequest()
        .withRecordType(FetchParsedRecordsBatchRequest.RecordType.MARC_BIB)
        .withConditions(conditions)
        .withData(data);

      recordService.fetchStrippedParsedRecords(batchRequest, TENANT_ID).onComplete(get -> {
        assertTrue(get.succeeded());
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> r.getExternalIdsHolder().getInstanceId().equals(externalId))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(expected.getFirst(), get.result().getRecords().getFirst());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFetchBibRecordsWithOneFieldByExternalId(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      assertTrue(batch.succeeded());

      String externalId = "3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc";
      List<FieldRange> data = List.of(
        new FieldRange().withFrom("001").withTo("001"),
        new FieldRange().withFrom("007").withTo("007")
      );
      String expectedContent =
        "{\"fields\": [{\"001\": \"inst000000000008\"}, {\"007\": \"cu\\\\uuu---uuuuu\"}]," +
        "\"leader\": \"01024nmm a2200277 ca4500\"}";

      Conditions conditions = new Conditions()
        .withIdType(IdType.INSTANCE.name())
        .withIds(List.of(externalId));
      FetchParsedRecordsBatchRequest batchRequest = new FetchParsedRecordsBatchRequest()
        .withRecordType(FetchParsedRecordsBatchRequest.RecordType.MARC_BIB)
        .withConditions(conditions)
        .withData(data);

      recordService.fetchStrippedParsedRecords(batchRequest, TENANT_ID).onComplete(get -> {
        assertTrue(get.succeeded());
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> r.getExternalIdsHolder().getInstanceId().equals(externalId))
          .peek(r -> r.getParsedRecord().setContent(expectedContent))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(expected.getFirst(), get.result().getRecords().getFirst());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFetchActualAndDeletedBibRecordsWithOneFieldByExternalIdWhenIncludeDeletedExists(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    records.get(3).setDeleted(true);
    records.get(3).setState(State.DELETED);
    records.get(5).setDeleted(true);
    records.get(5).setState(State.DELETED);
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
      }

      Set<String> externalIds = Set.of("3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc","6b4ae089-e1ee-431f-af83-e1133f8e3da0", "1b74ab75-9f41-4837-8662-a1d99118008d", "c1d3be12-ecec-4fab-9237-baf728575185", "8be05cf5-fb4f-4752-8094-8e179d08fb99");
      List<FieldRange> data = List.of(
        new FieldRange().withFrom("001").withTo("001"),
        new FieldRange().withFrom("007").withTo("007")
      );

      Conditions conditions = new Conditions()
        .withIdType(IdType.INSTANCE.name())
        .withIds(List.of("3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc", "6b4ae089-e1ee-431f-af83-e1133f8e3da0", "1b74ab75-9f41-4837-8662-a1d99118008d", "c1d3be12-ecec-4fab-9237-baf728575185", "8be05cf5-fb4f-4752-8094-8e179d08fb99"));
      FetchParsedRecordsBatchRequest batchRequest = new FetchParsedRecordsBatchRequest()
        .withRecordType(FetchParsedRecordsBatchRequest.RecordType.MARC_BIB)
        .withConditions(conditions)
        .withData(data)
        .withIncludeDeleted(true);

      recordService.fetchStrippedParsedRecords(batchRequest, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> externalIds.contains(r.getExternalIdsHolder().getInstanceId()))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFetchActualBibRecordsWithOneFieldByExternalIdWhenIncludeDeletedNotExists(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    records.get(3).setDeleted(true);
    records.get(3).setState(State.DELETED);
    records.get(5).setDeleted(true);
    records.get(5).setState(State.DELETED);
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
      }

      Set<String> externalIds = Set.of("3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc","6b4ae089-e1ee-431f-af83-e1133f8e3da0", "1b74ab75-9f41-4837-8662-a1d99118008d", "c1d3be12-ecec-4fab-9237-baf728575185", "8be05cf5-fb4f-4752-8094-8e179d08fb99");
      List<FieldRange> data = List.of(
        new FieldRange().withFrom("001").withTo("001"),
        new FieldRange().withFrom("007").withTo("007")
      );

      Conditions conditions = new Conditions()
        .withIdType(IdType.INSTANCE.name())
        .withIds(List.of("3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc", "6b4ae089-e1ee-431f-af83-e1133f8e3da0", "1b74ab75-9f41-4837-8662-a1d99118008d", "c1d3be12-ecec-4fab-9237-baf728575185", "8be05cf5-fb4f-4752-8094-8e179d08fb99"));
      FetchParsedRecordsBatchRequest batchRequest = new FetchParsedRecordsBatchRequest()
        .withRecordType(FetchParsedRecordsBatchRequest.RecordType.MARC_BIB)
        .withConditions(conditions)
        .withData(data);

      recordService.fetchStrippedParsedRecords(batchRequest, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> externalIds.contains(r.getExternalIdsHolder().getInstanceId()))
          .filter(r -> r.getState().equals(State.ACTUAL))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFetchActualAndDeletedBibRecordsWithOneFieldByExternalIdWhenIncludeDeletedFalse(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    records.get(3).setDeleted(true);
    records.get(3).setState(State.DELETED);
    records.get(5).setDeleted(true);
    records.get(5).setState(State.DELETED);
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
      }

      Set<String> externalIds = Set.of("3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc","6b4ae089-e1ee-431f-af83-e1133f8e3da0", "1b74ab75-9f41-4837-8662-a1d99118008d", "c1d3be12-ecec-4fab-9237-baf728575185", "8be05cf5-fb4f-4752-8094-8e179d08fb99");
      List<FieldRange> data = List.of(
        new FieldRange().withFrom("001").withTo("001"),
        new FieldRange().withFrom("007").withTo("007")
      );

      Conditions conditions = new Conditions()
        .withIdType(IdType.INSTANCE.name())
        .withIds(List.of("3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc", "6b4ae089-e1ee-431f-af83-e1133f8e3da0", "1b74ab75-9f41-4837-8662-a1d99118008d", "c1d3be12-ecec-4fab-9237-baf728575185", "8be05cf5-fb4f-4752-8094-8e179d08fb99"));
      FetchParsedRecordsBatchRequest batchRequest = new FetchParsedRecordsBatchRequest()
        .withRecordType(FetchParsedRecordsBatchRequest.RecordType.MARC_BIB)
        .withConditions(conditions)
        .withData(data)
        .withIncludeDeleted(false);

      recordService.fetchStrippedParsedRecords(batchRequest, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> externalIds.contains(r.getExternalIdsHolder().getInstanceId()))
          .filter(r -> r.getState().equals(State.ACTUAL))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldGetMarcAuthorityRecordsBySnapshotId(VertxTestContext testContext) {
    getRecordsBySnapshotId(testContext, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_AUTHORITY,
      Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldGetMarcHoldingsRecordsBySnapshotId(VertxTestContext testContext) {
    getRecordsBySnapshotId(testContext, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_HOLDING,
      Record.RecordType.MARC_HOLDING);
  }

  @Test
  void shouldGetEdifactRecordsBySnapshotId(VertxTestContext testContext) {
    getRecordsBySnapshotId(testContext, "dcd898af-03bb-4b12-b8a6-f6a02e86459b", RecordType.EDIFACT, Record.RecordType.EDIFACT);
  }

  @Test
  void shouldStreamMarcBibRecordsBySnapshotId(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
      }
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      Flowable<Record> flowable = recordService.streamRecords(condition, RecordType.MARC_BIB, orderFields, 0, 10, TENANT_ID);

      List<Record> expected = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
        .filter(r -> r.getSnapshotId().equals(snapshotId))
        .sorted(comparing(Record::getOrder))
        .toList();

      List<Record> actual = new ArrayList<>();
      flowable.doFinally(() -> {

          assertEquals(expected.size(), actual.size());
          compareRecords(expected.get(0), actual.get(0));
          compareRecords(expected.get(1), actual.get(1));
          compareRecords(expected.get(2), actual.get(2));

          testContext.completeNow();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  @Test
  void shouldCloseStreamRecordsTransactionWhenSubscriberCancels(VertxTestContext testContext) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));

      List<Record> expected = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
        .filter(r -> r.getSnapshotId().equals(snapshotId))
        .sorted(comparing(Record::getOrder))
        .toList();

      Flowable.range(0, 25)
        .concatMapSingle(ignored -> recordService
          .streamRecords(condition, RecordType.MARC_BIB, orderFields, 0, 10, TENANT_ID)
          .firstOrError())
        .ignoreElements()
        .andThen(recordService
          .streamRecords(condition, RecordType.MARC_BIB, orderFields, 0, 10, TENANT_ID)
          .toList())
        .subscribe(actual -> {
          assertEquals(expected.size(), actual.size());
          compareRecords(expected.get(0), actual.get(0));
          compareRecords(expected.get(1), actual.get(1));
          compareRecords(expected.get(2), actual.get(2));
          testContext.completeNow();
        }, testContext::failNow);
    });
  }

  @Test
  void shouldRollbackAndCloseWhenStreamRecordsQueryFails(VertxTestContext testContext) {
    Condition badCondition = DSL.field("nonexistent_column").eq("value");
    List<OrderField<?>> orderFields = new ArrayList<>();
    orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));

    Flowable.range(0, 5)
      .concatMapCompletable(ignored -> recordService
        .streamRecords(badCondition, RecordType.MARC_BIB, orderFields, 0, 10, TENANT_ID)
        .ignoreElements()
        .onErrorComplete())
      .andThen(recordService
        .streamRecords(RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString("ee561342-3098-47a8-ab6e-0f3eba120b04")),
          RecordType.MARC_BIB, orderFields, 0, 10, TENANT_ID)
        .ignoreElements())
      .subscribe(testContext::completeNow, testContext::failNow);
  }

  @Test
  void shouldStreamMarcAuthorityRecordsBySnapshotId(VertxTestContext testContext) {
    streamRecordsBySnapshotId(testContext, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_AUTHORITY,
      Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldStreamMarcHoldingsRecordsBySnapshotId(VertxTestContext testContext) {
    streamRecordsBySnapshotId(testContext, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_HOLDING,
      Record.RecordType.MARC_HOLDING);
  }

  @Test
  void shouldStreamEdifactRecordsBySnapshotId(VertxTestContext testContext) {
    streamRecordsBySnapshotId(testContext, "dcd898af-03bb-4b12-b8a6-f6a02e86459b", RecordType.EDIFACT,
      Record.RecordType.EDIFACT);
  }

  @Test
  void shouldGetMarcRecordsBetweenDates(VertxTestContext testContext) {
    getMarcRecordsBetweenDates(testContext, OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS),
      OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS).plusDays(1));
  }

  @Test
  void shouldGetMarcBibRecordById(VertxTestContext testContext) {
    getMarcRecordById(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldGetMarcAuthorityRecordById(VertxTestContext testContext) {
    getMarcRecordById(testContext, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  void shouldGetMarcHoldingsRecordById(VertxTestContext testContext) {
    getMarcRecordById(testContext, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  void shouldNotGetRecordById(VertxTestContext testContext) {
    Record expected = TestMocks.getRecord(0);
    recordService.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        testContext.failNow(get.cause());
      }
      assertFalse(get.result().isPresent());
      testContext.completeNow();
    });
  }

  @Test
  void shouldSaveMarcBibRecord(VertxTestContext testContext) {
    saveMarcRecord(testContext, TestMocks.getMarcBibRecord(), Record.RecordType.MARC_BIB);
  }

  @Test
  void shouldSaveMarcBibRecordWithMatchedIdFrom999field(VertxTestContext testContext) {
    String marc999 = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();
    ParsedRecord parsedRecord = new ParsedRecord().withId(marc999)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", marc999)))
          .put("ind1", "f")
          .put("ind2", "f"))).add(new JsonObject().put("001", HR_ID))).encode());
    Record rec = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(original.getRawRecord())
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(rec, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      assertNotNull(save.result().getRawRecord());
      assertNotNull(save.result().getParsedRecord());
      compareRecords(rec, save.result());
      recordDao.getRecordById(rec.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        assertEquals(marc999, get.result().get().getMatchedId());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFailDuringUpdateRecordGenerationIfIncomingMatchedIdNotEqualToMatchedIdFrom999field(VertxTestContext testContext) {
    String matchedId = UUID.randomUUID().toString();
    String marc999 = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();
    ParsedRecord parsedRecord = new ParsedRecord().withId(marc999)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", marc999)))
          .put("ind1", "f")
          .put("ind2", "f")))).encode());
    Record rec = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(original.getRawRecord())
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()))
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.updateRecordGeneration(matchedId, rec, okapiHeaders).onComplete(save -> {
      assertTrue(save.failed());
      assertTrue(save.cause() instanceof BadRequestException);
      recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertTrue(get.result().isEmpty());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFailDuringUpdateRecordGenerationIfRecordWithIdAsIncomingMatchedIfNotExist(VertxTestContext testContext) {
    String matchedId = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();
    ParsedRecord parsedRecord = new ParsedRecord().withId(matchedId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", matchedId)))
          .put("ind1", "f")
          .put("ind2", "f")))).encode());
    Record rec = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(original.getRawRecord())
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()))
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.updateRecordGeneration(matchedId, rec, okapiHeaders).onComplete(save -> {
      assertTrue(save.failed());
      assertTrue(save.cause() instanceof NotFoundException);
      recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertTrue(get.result().isEmpty());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldFailUpdateRecordGenerationIfDuplicateError(VertxTestContext testContext) {
    String matchedId = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();

    Record record1 = new Record()
      .withId(matchedId)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());

    Snapshot snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.PROCESSING_IN_PROGRESS);

    ParsedRecord parsedRecord = new ParsedRecord().withId(matchedId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", matchedId)))
          .put("ind1", "f")
          .put("ind2", "f"))).add(new JsonObject().put("001", HR_ID))).encode());
    Record recordToUpdateGeneration = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withGeneration(0)
      .withOrder(original.getOrder())
      .withRawRecord(original.getRawRecord())
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(record1, okapiHeaders).onComplete(record1Saved -> {
      if (record1Saved.failed()) {
        testContext.failNow(record1Saved.cause());
      }
      assertNotNull(record1Saved.result().getRawRecord());
      assertNotNull(record1Saved.result().getParsedRecord());
      assertEquals(record1Saved.result().getState(), State.ACTUAL);
      compareRecords(record1, record1Saved.result());

      SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(snapshotSaved -> {
        if (snapshotSaved.failed()) {
          testContext.failNow(snapshotSaved.cause());
        }
        recordService.updateRecordGeneration(matchedId, recordToUpdateGeneration, okapiHeaders).onComplete(recordToUpdateGenerationSaved -> {
          assertTrue(recordToUpdateGenerationSaved.failed());
          assertTrue(recordToUpdateGenerationSaved.cause() instanceof BadRequestException);
          testContext.completeNow();
        });
      });
    });
  }

  @Test
  void shouldUpdateRecordGeneration(VertxTestContext testContext) {
    String matchedId = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();

    Record record1 = new Record()
      .withId(matchedId)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());

    Snapshot snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.PROCESSING_IN_PROGRESS);

    ParsedRecord parsedRecord = new ParsedRecord().withId(matchedId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", matchedId)))
          .put("ind1", "f")
          .put("ind2", "f"))).add(new JsonObject().put("001", HR_ID))).encode());
    Record recordToUpdateGeneration = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(original.getRawRecord())
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(record1, okapiHeaders).onComplete(record1Saved -> {
      if (record1Saved.failed()) {
        testContext.failNow(record1Saved.cause());
      }
      assertNotNull(record1Saved.result().getRawRecord());
      assertNotNull(record1Saved.result().getParsedRecord());
      assertEquals(record1Saved.result().getState(), State.ACTUAL);
      compareRecords(record1, record1Saved.result());

      SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(snapshotSaved -> {
        if (snapshotSaved.failed()) {
          testContext.failNow(snapshotSaved.cause());
        }
        recordService.updateRecordGeneration(matchedId, recordToUpdateGeneration, okapiHeaders).onComplete(recordToUpdateGenerationSaved -> {
          verify(recordDomainEventPublisher).publishRecordUpdated(eq(record1Saved.result()), eq(recordToUpdateGenerationSaved.result()), any());
          assertTrue(recordToUpdateGenerationSaved.succeeded());
          assertEquals(recordToUpdateGenerationSaved.result().getMatchedId(), matchedId);
          assertEquals(recordToUpdateGenerationSaved.result().getGeneration(), 1);
          recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
            if (get.failed()) {
              testContext.failNow(get.cause());
            }
            assertTrue(get.result().isPresent());
            assertEquals(get.result().get().getGeneration(), 1);
            assertEquals(get.result().get().getMatchedId(), matchedId);
            assertNotEquals(get.result().get().getId(), matchedId);
            assertEquals(get.result().get().getState(), State.ACTUAL);
            recordDao.getRecordById(matchedId, TENANT_ID).onComplete(getRecord1 -> {
              if (getRecord1.failed()) {
                testContext.failNow(get.cause());
              }
              assertTrue(getRecord1.result().isPresent());
              assertEquals(getRecord1.result().get().getState(), State.OLD);
              testContext.completeNow();
            });
          });
        });
      });
    });
  }

  @Test
  void shouldFailUpdateRecordGenerationIfAnotherIncomingRecordOfSameJobAlreadyUpdatedRecord(VertxTestContext testContext) {
    String matchedId = UUID.randomUUID().toString();
    Record existingRecord = buildRecordToUpdateGeneration(matchedId, TestMocks.getMarcBibRecord().getSnapshotId(), null)
      .withId(matchedId);
    Snapshot snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.PROCESSING_IN_PROGRESS);
    Record firstIncomingRecord = buildRecordToUpdateGeneration(matchedId, snapshot.getJobExecutionId(), 1);
    Record secondIncomingRecord = buildRecordToUpdateGeneration(matchedId, snapshot.getJobExecutionId(), 2);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(existingRecord, okapiHeaders)
      .compose(v -> SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot))
      .compose(v -> recordService.updateRecordGeneration(matchedId, firstIncomingRecord, okapiHeaders))
      .onComplete(testContext.succeeding(firstUpdated -> {
        assertEquals(1, firstUpdated.getGeneration());
        assertEquals(snapshot.getJobExecutionId(), firstUpdated.getSnapshotId());

        recordService.updateRecordGeneration(matchedId, secondIncomingRecord, okapiHeaders).onComplete(secondUpdate -> {
          assertTrue(secondUpdate.failed());
          assertTrue(secondUpdate.cause() instanceof BadRequestException);
          assertEquals(UPDATE_RECORD_DUPLICATE_EXCEPTION, secondUpdate.cause().getMessage());
          recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(testContext.succeeding(get -> {
            assertTrue(get.isPresent());
            assertEquals(1, get.get().getGeneration());
            assertEquals(firstUpdated.getId(), get.get().getId());
            testContext.completeNow();
          }));
        });
      }));
  }

  @Test
  void shouldUpdateRecordGenerationTwiceWithinSameJobForSameIncomingRecord(VertxTestContext testContext) {
    String matchedId = UUID.randomUUID().toString();
    Record existingRecord = buildRecordToUpdateGeneration(matchedId, TestMocks.getMarcBibRecord().getSnapshotId(), null)
      .withId(matchedId);
    Snapshot snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.PROCESSING_IN_PROGRESS);
    Record incomingRecord = buildRecordToUpdateGeneration(matchedId, snapshot.getJobExecutionId(), 1);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(existingRecord, okapiHeaders)
      .compose(v -> SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot))
      .compose(v -> recordService.updateRecordGeneration(matchedId, incomingRecord, okapiHeaders))
      .compose(firstUpdated -> {
        assertEquals(1, firstUpdated.getGeneration());
        Record sameRecordNextAction = buildRecordToUpdateGeneration(matchedId, snapshot.getJobExecutionId(), 2)
          .withId(firstUpdated.getId());
        return recordService.updateRecordGeneration(matchedId, sameRecordNextAction, okapiHeaders);
      })
      .onComplete(testContext.succeeding(secondUpdated -> {
        assertEquals(2, secondUpdated.getGeneration());
        assertEquals(matchedId, secondUpdated.getMatchedId());
        recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(testContext.succeeding(get -> {
          assertTrue(get.isPresent());
          assertEquals(2, get.get().getGeneration());
          testContext.completeNow();
        }));
      }));
  }

  private Record buildRecordToUpdateGeneration(String matchedId, String snapshotId, Integer generation) {
    Record original = TestMocks.getMarcBibRecord();
    ParsedRecord parsedRecord = new ParsedRecord().withId(UUID.randomUUID().toString())
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", matchedId)))
          .put("ind1", "f")
          .put("ind2", "f"))).add(new JsonObject().put("001", HR_ID))).encode());
    return new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshotId)
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withGeneration(generation)
      .withOrder(original.getOrder())
      .withRawRecord(original.getRawRecord())
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());
  }

  @Test
  void shouldUpdateRecordGenerationByMatchId(VertxTestContext testContext) {
    var mock = TestMocks.getMarcBibRecord();
    var recordToSave = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(mock.getSnapshotId())
      .withRecordType(mock.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(mock.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(mock.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(mock.getMetadata());

    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(recordToSave, okapiHeaders).onComplete(savedRecord -> {
      if (savedRecord.failed()) {
        testContext.failNow(savedRecord.cause());
      }
      assertNotNull(savedRecord.result().getRawRecord());
      assertNotNull(savedRecord.result().getParsedRecord());
      assertEquals(savedRecord.result().getState(), State.ACTUAL);
      compareRecords(recordToSave, savedRecord.result());

      var matchedId = savedRecord.result().getMatchedId();
      var snapshot = new Snapshot().withJobExecutionId(UUID.randomUUID().toString())
        .withProcessingStartedDate(new Date())
        .withStatus(Snapshot.Status.PROCESSING_IN_PROGRESS);

      var parsedRecord = new ParsedRecord().withId(UUID.randomUUID().toString())
        .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
          .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
            .put("subfields",
              new JsonArray().add(new JsonObject().put("s", matchedId)))
            .put("ind1", "f")
            .put("ind2", "f"))).add(new JsonObject().put("001", HR_ID))).encode());

      var recordToUpdateGeneration = new Record()
        .withId(UUID.randomUUID().toString())
        .withSnapshotId(snapshot.getJobExecutionId())
        .withRecordType(mock.getRecordType())
        .withState(State.ACTUAL)
        .withOrder(mock.getOrder())
        .withRawRecord(mock.getRawRecord())
        .withParsedRecord(parsedRecord)
        .withAdditionalInfo(mock.getAdditionalInfo())
        .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
        .withMetadata(mock.getMetadata());

      SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(snapshotSaved -> {
        if (snapshotSaved.failed()) {
          testContext.failNow(snapshotSaved.cause());
        }

        recordService.updateRecordGeneration(matchedId, recordToUpdateGeneration, okapiHeaders).onComplete(recordToUpdateGenerationSaved -> {
          assertTrue(recordToUpdateGenerationSaved.succeeded());
          assertEquals(recordToUpdateGenerationSaved.result().getMatchedId(), matchedId);
          assertEquals(recordToUpdateGenerationSaved.result().getGeneration(), 1);
          recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
            if (get.failed()) {
              testContext.failNow(get.cause());
            }
            assertTrue(get.result().isPresent());
            assertEquals(get.result().get().getGeneration(), 1);
            assertEquals(get.result().get().getMatchedId(), matchedId);
            assertNotEquals(get.result().get().getId(), matchedId);
            assertEquals(get.result().get().getState(), State.ACTUAL);
            recordDao.getRecordById(matchedId, TENANT_ID).onComplete(getRecord1 -> {
              if (getRecord1.failed()) {
                testContext.failNow(get.cause());
              }
              assertTrue(getRecord1.result().isPresent());
              assertEquals(getRecord1.result().get().getState(), State.OLD);
              testContext.completeNow();
            });
          });
        });
      });
    });
  }

  @Test
  void shouldSaveMarcBibRecordWithMatchedIdFromRecordId(VertxTestContext testContext) {
    Record original = TestMocks.getMarcBibRecord();
    String recordId = UUID.randomUUID().toString();

    Record rec = new Record()
      .withId(recordId)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID))
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(rec, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      assertNotNull(save.result().getRawRecord());
      assertNotNull(save.result().getParsedRecord());
      compareRecords(rec, save.result());
      recordDao.getRecordById(rec.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        assertEquals(recordId, get.result().get().getMatchedId());
        assertEquals(getFieldFromMarcRecord(get.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S), recordId);
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldSaveEdifactRecordAndNotSet999Field(VertxTestContext testContext) {
    Record rec = TestMocks.getRecords(Record.RecordType.EDIFACT);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(rec, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      recordDao.getRecordById(rec.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        assertEquals(rec.getId(), get.result().get().getMatchedId());
        assertNull(getFieldFromMarcRecord(get.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S));
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldSaveMarcBibRecordWithMatchedIdFromExistingSourceRecord(VertxTestContext testContext) {
    Record original = TestMocks.getMarcBibRecord();
    String recordId1 = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(HR_ID);
    Record record1 = new Record()
      .withId(recordId1)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(externalIdsHolder)
      .withMetadata(original.getMetadata());

    ParsedRecord parsedRecord2 = new ParsedRecord()
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
    String recordId2 = UUID.randomUUID().toString();
    Record record2 = new Record()
      .withId(recordId2)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord2)
      .withGeneration(1)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(externalIdsHolder)
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(record1, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      assertNotNull(save.result().getRawRecord());
      assertNotNull(save.result().getParsedRecord());
      compareRecords(record1, save.result());
      recordDao.getRecordById(record1.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        assertEquals(recordId1, get.result().get().getMatchedId());
        assertEquals(getFieldFromMarcRecord(get.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S), recordId1);

        recordService.saveRecord(record2, okapiHeaders).onComplete(save2 -> {
          if (save2.failed()) {
            testContext.failNow(save2.cause());
          }
          assertNotNull(save2.result().getRawRecord());
          assertNotNull(save2.result().getParsedRecord());
          compareRecords(record2, save2.result());
          recordDao.getRecordById(record2.getId(), TENANT_ID).onComplete(get2 -> {
            if (get2.failed()) {
              testContext.failNow(get2.cause());
            }
            assertTrue(get2.result().isPresent());
            assertNotNull(get2.result().get().getRawRecord());
            assertNotNull(get2.result().get().getParsedRecord());
            assertEquals(recordId1, get2.result().get().getMatchedId());
            assertEquals(getFieldFromMarcRecord(get2.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S), recordId1);
            testContext.completeNow();
          });
        });
      });
    });
  }

  @Test
  void shouldSaveMarcAuthorityRecord(VertxTestContext testContext) {
    saveMarcRecord(testContext, TestMocks.getMarcAuthorityRecord(), Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldSaveMarcHoldingsRecord(VertxTestContext testContext) {
    saveMarcRecord(testContext, TestMocks.getMarcHoldingsRecord(), Record.RecordType.MARC_HOLDING);
  }

  @Test
  void shouldSaveEdifactRecord(VertxTestContext testContext) {
    saveMarcRecord(testContext, TestMocks.getEdifactRecord(), Record.RecordType.EDIFACT);
  }

  @Test
  void shouldSaveMarcBibRecordWithGenerationGreaterThanZero(VertxTestContext testContext) {
    saveMarcRecordWithGenerationGreaterThanZero(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldFailToSaveRecord(VertxTestContext testContext) {
    Record valid = TestMocks.getRecord(0);
    String fakeSnapshotId = "fakeId";
    Record invalid = new Record()
      .withId(valid.getId())
      .withSnapshotId(fakeSnapshotId)
      .withRecordType(valid.getRecordType())
      .withState(valid.getState())
      .withGeneration(valid.getGeneration())
      .withOrder(valid.getOrder())
      .withRawRecord(valid.getRawRecord())
      .withParsedRecord(valid.getParsedRecord())
      .withAdditionalInfo(valid.getAdditionalInfo())
      .withExternalIdsHolder(valid.getExternalIdsHolder())
      .withMetadata(valid.getMetadata());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordService.saveRecord(invalid, okapiHeaders).onComplete(save -> {
      assertTrue(save.failed());
      String expected = "Invalid UUID string: " + fakeSnapshotId;
      assertTrue(save.cause().getMessage().contains(expected));
      testContext.completeNow();
    });
  }

  @Test
  void shouldSaveMarcBibRecords(VertxTestContext testContext) {
    saveMarcRecords(testContext, Record.RecordType.MARC_BIB);
  }

  @Test
  void shouldSaveMarcAuthorityRecords(VertxTestContext testContext) {
    saveMarcRecords(testContext, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldSaveEdifactRecords(VertxTestContext testContext) {
    saveMarcRecords(testContext, Record.RecordType.EDIFACT);
  }

  @Test
  void shouldSaveMarcBibRecordsWithExpectedErrors(VertxTestContext testContext) {
    saveMarcRecordsWithExpectedErrors(testContext);
  }

  @Test
  void shouldUpdateMarcRecord(VertxTestContext testContext) {
    Record original = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordDao.saveRecord(original, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      Record expected = new Record()
        .withId(original.getId())
        .withSnapshotId(original.getSnapshotId())
        .withMatchedId(original.getMatchedId())
        .withRecordType(original.getRecordType())
        .withState(State.OLD)
        .withGeneration(original.getGeneration())
        .withOrder(original.getOrder())
        .withRawRecord(original.getRawRecord())
        .withParsedRecord(original.getParsedRecord())
        .withAdditionalInfo(original.getAdditionalInfo())
        .withExternalIdsHolder(original.getExternalIdsHolder())
        .withMetadata(original.getMetadata());
      recordService.updateRecord(expected, okapiHeaders).onComplete(update -> {
        if (update.failed()) {
          testContext.failNow(update.cause());
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordUpdated(eq(save.result()), eq(update.result()), any());
        assertTrue(update.result().getMetadata().getUpdatedDate()
          .after(update.result().getMetadata().getCreatedDate()));
        assertNotNull(update.result().getRawRecord());
        assertNotNull(update.result().getParsedRecord());
        assertNull(update.result().getErrorRecord());
        compareRecords(expected, update.result());
        Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(expected.getMatchedId()))
          .and(RECORDS_LB.STATE.eq(RecordState.OLD));
        recordDao.getRecordByCondition(condition, TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
          }
          assertTrue(get.result().isPresent());
          testContext.completeNow();
        });
      });
    });
  }

  @Test
  void shouldUpdateParsedRecord(VertxTestContext testContext) {
    Record original = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordDao.saveRecord(original, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      Record expected = new Record()
        .withId(original.getId())
        .withSnapshotId(original.getSnapshotId())
        .withMatchedId(original.getMatchedId())
        .withRecordType(original.getRecordType())
        .withState(State.OLD)
        .withGeneration(original.getGeneration())
        .withOrder(original.getOrder())
        .withRawRecord(original.getRawRecord())
        .withParsedRecord(original.getParsedRecord())
        .withAdditionalInfo(original.getAdditionalInfo())
        .withExternalIdsHolder(original.getExternalIdsHolder())
        .withMetadata(original.getMetadata());
      recordService.updateParsedRecord(expected, okapiHeaders).onComplete(update -> {
        if (update.failed()) {
          testContext.failNow(update.cause());
        }
        recordService.getRecordById(expected.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
          }
          assertTrue(get.result().isPresent());

          ArgumentCaptor<Record> captureOldRecord = ArgumentCaptor.forClass(Record.class);
          ArgumentCaptor<Record> captureNewRecord = ArgumentCaptor.forClass(Record.class);
          Record expectedNewRecord = MarcUtil.clone(get.result().get(), Record.class).withErrorRecord(null).withRawRecord(null);

          verify(recordDomainEventPublisher, times(1))
            .publishRecordUpdated(captureOldRecord.capture(), captureNewRecord.capture(), any());

          compareRecords(captureOldRecord.getValue(), save.result());
          compareRecords(captureNewRecord.getValue(), expectedNewRecord);
          testContext.completeNow();
        });
      });
    });
  }

  @Test
  void shouldUpdateEdifactRecord(VertxTestContext testContext) {
    Record original = TestMocks.getEdifactRecord();
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      Record expected = new Record()
        .withId(original.getId())
        .withSnapshotId(original.getSnapshotId())
        .withMatchedId(original.getMatchedId())
        .withRecordType(original.getRecordType())
        .withState(State.OLD)
        .withGeneration(original.getGeneration())
        .withOrder(original.getOrder())
        .withRawRecord(original.getRawRecord())
        .withParsedRecord(original.getParsedRecord())
        .withAdditionalInfo(original.getAdditionalInfo())
        .withExternalIdsHolder(original.getExternalIdsHolder())
        .withMetadata(original.getMetadata());
      recordService.updateRecord(expected, okapiHeaders).onComplete(update -> {
        if (update.failed()) {
          testContext.failNow(update.cause());
        }
        assertTrue(update.result().getMetadata().getUpdatedDate()
          .after(update.result().getMetadata().getCreatedDate()));
        assertNotNull(update.result().getRawRecord());
        assertNotNull(update.result().getParsedRecord());
        assertNull(update.result().getErrorRecord());
        compareRecords(expected, update.result());
        Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(expected.getMatchedId()))
          .and(RECORDS_LB.STATE.eq(RecordState.OLD));
        recordDao.getRecordByCondition(condition, TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
          }
          assertTrue(get.result().isPresent());
          testContext.completeNow();
        });
      });
    });
  }

  @Test
  void shouldFailToUpdateRecord(VertxTestContext testContext) {
    Record rec = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.getRecordById(rec.getMatchedId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        testContext.failNow(get.cause());
      }
      assertFalse(get.result().isPresent());
      recordService.updateRecord(rec, okapiHeaders).onComplete(update -> {
        assertTrue(update.failed());
        String expected = String.format("Record with id '%s' was not found", rec.getId());
        assertEquals(expected, update.cause().getMessage());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldGetMarcBibSourceRecords(VertxTestContext testContext) {
    getMarcSourceRecords(testContext, RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  void shouldGetMarcAuthoritySourceRecords(VertxTestContext testContext) {
    getMarcSourceRecords(testContext, RecordType.MARC_AUTHORITY, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldGetEdifactSourceRecords(VertxTestContext testContext) {
    getMarcSourceRecords(testContext, RecordType.EDIFACT, Record.RecordType.EDIFACT);
  }

  @Test
  void shouldStreamMarcBibSourceRecords(VertxTestContext testContext) {
    streamMarcSourceRecords(testContext, RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  void shouldStreamMarcAuthoritySourceRecords(VertxTestContext testContext) {
    streamMarcSourceRecords(testContext, RecordType.MARC_AUTHORITY, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldStreamMarcHoldingSourceRecords(VertxTestContext testContext) {
    streamMarcSourceRecords(testContext, RecordType.MARC_HOLDING, Record.RecordType.MARC_HOLDING);
  }

  @Test
  void shouldStreamEdifactSourceRecords(VertxTestContext testContext) {
    streamMarcSourceRecords(testContext, RecordType.EDIFACT, Record.RecordType.EDIFACT);
  }

  @Test
  void shouldGetMarcBibSourceRecordsByListOfIds(VertxTestContext testContext) {
    getMarcSourceRecordsByListOfIds(testContext, Record.RecordType.MARC_BIB, RecordType.MARC_BIB);
  }

  @Test
  void shouldGetMarcAuthoritySourceRecordsByListOfIds(VertxTestContext testContext) {
    getMarcSourceRecordsByListOfIds(testContext, Record.RecordType.MARC_AUTHORITY, RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldGetMarcHoldingsSourceRecordsByListOfIds(VertxTestContext testContext) {
    getMarcSourceRecordsByListOfIds(testContext, Record.RecordType.MARC_HOLDING, RecordType.MARC_HOLDING);
  }

  @Test
  void shouldGetMarcBibSourceRecordsByListOfIdsThatAreDeleted(VertxTestContext testContext) {
    getMarcSourceRecordsByListOfIdsThatAreDeleted(testContext, Record.RecordType.MARC_BIB, RecordType.MARC_BIB);
  }

  @Test
  void shouldGetMarcAuthoritySourceRecordsByListOfIdsThatAreDeleted(VertxTestContext testContext) {
    getMarcSourceRecordsByListOfIdsThatAreDeleted(testContext, Record.RecordType.MARC_AUTHORITY, RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldGetMarcHoldingsSourceRecordsByListOfIdsThatAreDeleted(VertxTestContext testContext) {
    getMarcSourceRecordsByListOfIdsThatAreDeleted(testContext, Record.RecordType.MARC_HOLDING, RecordType.MARC_HOLDING);
  }

  @Test
  void shouldGetMarcBibSourceRecordsBetweenDates(VertxTestContext testContext) {
    getMarcSourceRecordsBetweenDates(testContext,
      OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS), OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS).plusDays(1));
  }

  @Test
  void shouldGetMarcBibSourceRecordById(VertxTestContext testContext) {
    getMarcSourceRecordById(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldGetMarcAuthoritySourceRecordById(VertxTestContext testContext) {
    getMarcSourceRecordById(testContext, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  void shouldGetMarcHoldingsSourceRecordById(VertxTestContext testContext) {
    getMarcSourceRecordById(testContext, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  void shouldNotGetMarcBibSourceRecordById(VertxTestContext testContext) {
    notGetMarcSourceRecordById(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldNotGetMarcAuthoritySourceRecordById(VertxTestContext testContext) {
    notGetMarcSourceRecordById(testContext, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  void shouldNotGetMarcHoldingsSourceRecordById(VertxTestContext testContext) {
    notGetMarcSourceRecordById(testContext, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  void shouldUpdateParsedMarcBibRecords(VertxTestContext testContext) {
    updateParsedMarcRecords(testContext, Record.RecordType.MARC_BIB);
  }

  @Test
  void shouldUpdateParsedMarcAuthorityRecords(VertxTestContext testContext) {
    updateParsedMarcRecords(testContext, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldUpdateParsedMarcHoldingsRecords(VertxTestContext testContext) {
    updateParsedMarcRecords(testContext, Record.RecordType.MARC_HOLDING);
  }

  @Test
  void shouldUpdateParsedMarcBibRecordsAndGetOnlyActualRecord(VertxTestContext testContext) {
    updateParsedMarcRecordsAndGetOnlyActualRecord(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldUpdateParsedMarcAuthorityRecordsAndGetOnlyActualRecord(VertxTestContext testContext) {
    updateParsedMarcRecordsAndGetOnlyActualRecord(testContext, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  void shouldUpdateParsedMarcHoldingsRecordsAndGetOnlyActualRecord(VertxTestContext testContext) {
    updateParsedMarcRecordsAndGetOnlyActualRecord(testContext, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  void shouldGetFormattedMarcBibRecord(VertxTestContext testContext) {
    getFormattedMarcRecord(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldGetFormattedMarcAuthorityRecord(VertxTestContext testContext) {
    getFormattedMarcRecord(testContext, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  void shouldGetFormattedMarcHoldingsRecord(VertxTestContext testContext) {
    getFormattedMarcRecord(testContext, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  void shouldGetFormattedEdifactRecord(VertxTestContext testContext) {
    Record expected = TestMocks.getEdifactRecord();
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      recordService.getFormattedRecord(expected.getId(), IdType.RECORD, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertNotNull(get.result().getParsedRecord());
        assertEquals(expected.getParsedRecord().getFormattedContent(),
          get.result().getParsedRecord().getFormattedContent());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldGetFormattedDeletedRecord(VertxTestContext testContext) {
    Record expected = TestMocks.getMarcBibRecord();
    expected.setState(State.DELETED);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }
      recordService.getFormattedRecord(expected.getId(), IdType.RECORD, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
        }
        assertNotNull(get.result().getParsedRecord());
        assertEquals(expected.getParsedRecord().getFormattedContent(),
          get.result().getParsedRecord().getFormattedContent());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldUpdateSuppressFromDiscoveryForMarcBibRecord(VertxTestContext testContext) {
    updateSuppressFromDiscoveryForMarcRecord(testContext, TestMocks.getMarcBibRecord());
  }

  @Test
  void shouldUpdateSuppressFromDiscoveryForMarcAuthorityRecord(VertxTestContext testContext) {
    updateSuppressFromDiscoveryForMarcRecord(testContext, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  void shouldUpdateSuppressFromDiscoveryForMarcHoldingsRecord(VertxTestContext testContext) {
    updateSuppressFromDiscoveryForMarcRecord(testContext, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  void shouldDeleteMarcBibRecordsBySnapshotId(VertxTestContext testContext) {
    deleteMarcRecordsBySnapshotId(testContext, MARC_BIB_RECORD_SNAPSHOT_ID, RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  void shouldDeleteMarcAuthorityRecordsBySnapshotId(VertxTestContext testContext) {
    deleteMarcRecordsBySnapshotId(testContext, MARC_AUTHORITY_RECORD_SNAPSHOT_ID, RecordType.MARC_AUTHORITY, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldGetNoRecordsWithLimitEqualsZero(VertxTestContext testContext) {
    getTotalRecordsAndRecordsDependsOnLimit(testContext, 0);
  }

  @Test
  void shouldGetNoRecordsWithLimitNotEqualsZero(VertxTestContext testContext) {
    getTotalRecordsAndRecordsDependsOnLimit(testContext, 1);
  }

  @Test
  void shouldThrowExceptionWhenSavedDuplicateRecord(VertxTestContext testContext) {
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(rec -> rec.getRecordType().equals(Record.RecordType.MARC_BIB))
      .map(rec -> rec.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .toList();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    vertx.runOnContext(v -> {
    List<Future<RecordsBatchResponse>> futures = List.of(recordService.saveRecords(recordCollection, okapiHeaders),
      recordService.saveRecords(recordCollection, okapiHeaders));

    Future.all(futures).onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.failed());
      // In Vert.x 5, CompositeFuture may wrap causes. The actual exception is in ar.cause()
      // or in one of the individual futures. Check both the direct cause and its chain.
      Throwable cause = ar.cause();
      boolean isDuplicateException = false;
      while (cause != null) {
        if (cause instanceof DuplicateEventException) {
          isDuplicateException = true;
          break;
        }
        cause = cause.getCause();
      }
      if (!isDuplicateException) {
        // also check individual futures
        for (var f : futures) {
          if (f.failed()) {
            cause = f.cause();
            while (cause != null) {
              if (cause instanceof DuplicateEventException) {
                isDuplicateException = true;
                break;
              }
              cause = cause.getCause();
            }
          }
          if (isDuplicateException) break;
        }
      }
      assertTrue(isDuplicateException, "Expected DuplicateEventException but got: " + ar.cause());
      testContext.completeNow();
    }));
    });
  }

  @Test
  void shouldHardDeleteMarcRecord(VertxTestContext testContext) {
    Record original = TestMocks.getMarcBibRecord();
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(HR_ID);
    Record sourceRecord = new Record()
      .withId(recordId)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(externalIdsHolder)
      .withMetadata(original.getMetadata());

    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(sourceRecord, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }

      recordService.deleteRecordsByExternalId(sourceRecord.getExternalIdsHolder().getInstanceId(), okapiHeaders).onComplete(delete -> {
        if (delete.failed()) {
          testContext.failNow(delete.cause());
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordDeleted(eq(save.result()), any());

        recordService.getRecordById(sourceRecord.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
          }
          assertTrue(get.result().isEmpty());
        });
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldUnDeleteMarcRecord(VertxTestContext testContext) {
    var marcBibMock = TestMocks.getMarcBibRecord();
    var sourceRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(marcBibMock.getSnapshotId())
      .withRecordType(marcBibMock.getRecordType())
      .withState(State.DELETED)
      .withOrder(marcBibMock.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(marcBibMock.getAdditionalInfo())
      .withExternalIdsHolder(
        new ExternalIdsHolder()
          .withInstanceId(UUID.randomUUID().toString())
          .withInstanceHrid("12345abcd"))
      .withMetadata(marcBibMock.getMetadata());

    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(sourceRecord, okapiHeaders).onComplete(saveResult -> {
      if (saveResult.failed()) {
        testContext.failNow(saveResult.cause());
      }
      recordService.unDeleteRecordById(sourceRecord.getId(), IdType.RECORD, okapiHeaders).onComplete(undeleteResult -> {
        if (undeleteResult.failed()) {
          testContext.failNow(undeleteResult.cause());
        }
        recordService.getRecordById(sourceRecord.getId(), TENANT_ID).onComplete(getResult -> {
          if (getResult.failed()) {
            testContext.failNow(getResult.cause());
          }
          assertTrue(getResult.result().isPresent());
          assertFalse(getResult.result().get().getDeleted());
          verify(recordDomainEventPublisher, times(1))
            .publishRecordUpdated(eq(saveResult.result()), eq(getResult.result().get()), any());
        });
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldSoftDeleteMarcRecord(VertxTestContext testContext) {
    Record original = TestMocks.getMarcBibRecord();
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(HR_ID);
    Record sourceRecord = new Record()
      .withId(recordId)
      .withSnapshotId(original.getSnapshotId())
      .withRecordType(original.getRecordType())
      .withState(State.ACTUAL)
      .withOrder(original.getOrder())
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(externalIdsHolder)
      .withMetadata(original.getMetadata());

    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(sourceRecord, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
      }

      recordService.deleteRecordById(sourceRecord.getId(), IdType.RECORD, okapiHeaders).onComplete(delete -> {
        if (delete.failed()) {
          testContext.failNow(delete.cause());
        }
        recordService.getRecordById(sourceRecord.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
          }

          assertTrue(get.result().isPresent());
          assertTrue(get.result().get().getDeleted());
          verify(recordDomainEventPublisher, times(1))
            .publishRecordUpdated(eq(save.result()), eq(get.result().get()), any());
        });
        testContext.completeNow();
      });
    });
  }

  private void getTotalRecordsAndRecordsDependsOnLimit(VertxTestContext testContext, int limit) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ID.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.MARC_BIB, orderFields, 0, limit, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        assertEquals(limit, get.result().getRecords().size());
        testContext.completeNow();
      });
    });
  }

  private void getRecordsBySnapshotId(VertxTestContext testContext, String snapshotId, RecordType parsedRecordType,
                                      Record.RecordType recordType) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, parsedRecordType, orderFields, 0, 1, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .sorted(comparing(Record::getOrder))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(expected.getFirst(), get.result().getRecords().getFirst());
        testContext.completeNow();
      });
    });
  }

  private void getMarcRecordsBetweenDates(VertxTestContext testContext, OffsetDateTime earliestDate, OffsetDateTime latestDate) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = RECORDS_LB.CREATED_DATE.between(earliestDate, latestDate);
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.MARC_BIB, orderFields, 0, 15, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(expected, get.result().getRecords());
        testContext.completeNow();
      });
    });
  }

  private void streamRecordsBySnapshotId(VertxTestContext testContext, String snapshotId, RecordType parsedRecordType,
                                         Record.RecordType recordType) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      Flowable<Record> flowable = recordService.streamRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID);

      List<Record> expected = records.stream()
        .filter(r -> r.getRecordType().equals(recordType))
        .filter(r -> r.getSnapshotId().equals(snapshotId))
        .sorted(comparing(Record::getOrder))
        .toList();

      List<Record> actual = new ArrayList<>();
      flowable.doFinally(() -> {

          assertEquals(expected.size(), actual.size());
          compareRecords(expected.getFirst(), actual.getFirst());

          testContext.completeNow();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  private void getMarcRecordById(VertxTestContext testContext, Record expected) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      recordService.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        assertTrue(get.succeeded());
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        compareRecords(expected, get.result().get());
        testContext.completeNow();
      });
    });
  }

  private void saveMarcRecord(VertxTestContext testContext, Record expected, Record.RecordType marcBib) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      assertNotNull(save.result().getRawRecord());
      assertNotNull(save.result().getParsedRecord());
      compareRecords(expected, save.result());
      recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordCreated(eq(save.result()), any());
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        assertEquals(marcBib, get.result().get().getRecordType());
        compareRecords(expected, get.result().get());
        testContext.completeNow();
      });
    });
  }

  private void saveMarcRecordWithGenerationGreaterThanZero(VertxTestContext testContext, Record expected) {
    expected.setGeneration(1);
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordService.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      assertNotNull(save.result().getRawRecord());
      assertNotNull(save.result().getParsedRecord());
      compareRecords(expected, save.result());
      recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        assertTrue(get.result().isPresent());
        assertNotNull(get.result().get().getRawRecord());
        assertNotNull(get.result().get().getParsedRecord());
        assertEquals(Record.RecordType.MARC_BIB, get.result().get().getRecordType());
        assertTrue(get.result().get().getGeneration() > 0);
        compareRecords(expected, get.result().get());
        testContext.completeNow();
      });
    });
  }

  private void saveMarcRecords(VertxTestContext testContext, Record.RecordType marcBib) {
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(rec -> rec.getRecordType().equals(marcBib))
      .map(rec -> rec.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .toList();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    vertx.runOnContext(v -> {
      recordService.saveRecords(recordCollection, okapiHeaders).onComplete(testContext.succeeding(batch -> testContext.verify(() -> {
        ArgumentCaptor<Record> captureOldRecord = ArgumentCaptor.forClass(Record.class);
        verify(recordDomainEventPublisher, times(batch.getTotalRecords())).publishRecordCreated(captureOldRecord.capture(), any());
        compareRecords(captureOldRecord.getAllValues(), expected);
        assertEquals(0, batch.getErrorMessages().size());
        assertEquals(expected.size(), batch.getTotalRecords());
        compareRecords(expected, batch.getRecords());
        RecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition())
          .onComplete(count -> testContext.verify(() -> {
            assertTrue(count.succeeded());
            assertEquals(expected.size(), count.result());
            testContext.completeNow();
          }));
      })));
    });
  }

  private void saveMarcRecordsWithExpectedErrors(VertxTestContext testContext) {
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(rec -> rec.getRecordType().equals(Record.RecordType.MARC_BIB))
      .map(rec -> rec.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .map(rec -> rec.withErrorRecord(TestMocks.getErrorRecord(0)))
      .toList();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    vertx.runOnContext(v -> {
      recordService.saveRecords(recordCollection, okapiHeaders).onComplete(testContext.succeeding(batch -> testContext.verify(() -> {
        assertEquals(0, batch.getErrorMessages().size());
        assertEquals(expected.size(), batch.getTotalRecords());
        compareRecords(expected, batch.getRecords());
        checkRecordErrorRecords(batch.getRecords(), TestMocks.getErrorRecord(0).getContent().toString(),
          TestMocks.getErrorRecord(0).getDescription());
        RecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition())
          .onComplete(count -> testContext.verify(() -> {
            assertTrue(count.succeeded());
            assertEquals(expected.size(), count.result());
            testContext.completeNow();
          }));
      })));
    });
  }

  private void checkRecordErrorRecords(List<Record> actual, String expectedErrorContent,
                                       String expectedErrorDescription) {
    for (Record rec : actual) {
      assertEquals(expectedErrorContent, rec.getErrorRecord().getContent());
      assertEquals(expectedErrorDescription, rec.getErrorRecord().getDescription());
    }
  }

  private void getMarcSourceRecords(VertxTestContext testContext, RecordType parsedRecordType, Record.RecordType recordType) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }

      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(expected, get.result().getSourceRecords());
        testContext.completeNow();
      });
    });
  }

  private void streamMarcSourceRecords(VertxTestContext testContext, RecordType parsedRecordType, Record.RecordType recordType) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();

      Flowable<SourceRecord> flowable = recordService
        .streamSourceRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID);

      List<SourceRecord> expected = records.stream()
        .filter(r -> r.getRecordType().equals(recordType))
        .map(RecordDaoUtil::toSourceRecord)
        .toList();

      List<SourceRecord> actual = new ArrayList<>();
      flowable.doFinally(() -> {
          assertEquals(expected.size(), actual.size());
          compareSourceRecords(expected, actual);

          testContext.completeNow();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  private void getMarcSourceRecordsByListOfIds(VertxTestContext testContext, Record.RecordType recordType,
                                               RecordType parsedRecordType) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      List<String> ids = records.stream()
        .filter(r -> r.getRecordType().equals(recordType))
        .map(Record::getMatchedId)
        .toList();

      recordService.getSourceRecords(ids, IdType.RECORD, parsedRecordType, false, false, okapiHeaders).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(expected, get.result().getSourceRecords());
        testContext.completeNow();
      });
    });
  }

  private void getMarcSourceRecordsBetweenDates(VertxTestContext testContext,
                                                OffsetDateTime earliestDate,
                                                OffsetDateTime latestDate) {
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }

      Condition condition = RECORDS_LB.CREATED_DATE.between(earliestDate, latestDate);
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, RecordType.MARC_BIB, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(expected, get.result().getSourceRecords());
        testContext.completeNow();
      });
    });
  }

  private void getMarcSourceRecordsByListOfIdsThatAreDeleted(VertxTestContext testContext, Record.RecordType recordType,
                                                             RecordType parsedRecordType) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    List<Record> records = TestMocks.getRecords().stream()
      .map(rec -> {
        Record deletedRecord = new Record()
          .withId(rec.getId())
          .withSnapshotId(rec.getSnapshotId())
          .withMatchedId(rec.getMatchedId())
          .withRecordType(rec.getRecordType())
          .withState(State.DELETED)
          .withGeneration(rec.getGeneration())
          .withOrder(rec.getOrder())
          .withLeaderRecordStatus(rec.getLeaderRecordStatus())
          .withRawRecord(rec.getRawRecord())
          .withParsedRecord(rec.getParsedRecord())
          .withAdditionalInfo(rec.getAdditionalInfo())
          .withExternalIdsHolder(rec.getExternalIdsHolder());
        if (Objects.nonNull(rec.getMetadata())) {
          deletedRecord.withMetadata(rec.getMetadata());
        }
        if (Objects.nonNull(rec.getErrorRecord())) {
          deletedRecord.withErrorRecord(rec.getErrorRecord());
        }
        return deletedRecord;
      })
      .toList();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      List<String> ids = records.stream()
        .filter(r -> r.getRecordType().equals(recordType))
        .map(Record::getMatchedId)
        .toList();
      recordService.getSourceRecords(ids, IdType.RECORD, parsedRecordType, true, false, okapiHeaders).onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(expected, get.result().getSourceRecords());
        testContext.completeNow();
      });
    });
  }

  private void getMarcSourceRecordById(VertxTestContext testContext, Record expected) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      recordService
        .getSourceRecordById(expected.getMatchedId(), IdType.RECORD, RecordState.ACTUAL, TENANT_ID)
        .onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
            return;
          }
          assertTrue(get.result().isPresent());
          assertNotNull(get.result().get().getParsedRecord());
          compareSourceRecords(RecordDaoUtil.toSourceRecord(expected), get.result().get());
          testContext.completeNow();
        });
    });
  }

  private void notGetMarcSourceRecordById(VertxTestContext testContext, Record expected) {
    recordService
      .getSourceRecordById(expected.getMatchedId(), IdType.RECORD, RecordState.ACTUAL, TENANT_ID)
      .onComplete(get -> {
        if (get.failed()) {
          testContext.failNow(get.cause());
          return;
        }
        assertFalse(get.result().isPresent());
        testContext.completeNow();
      });
  }

  private void updateParsedMarcRecords(VertxTestContext testContext, Record.RecordType recordType) {
    List<Record> original = TestMocks.getRecords().stream()
      .filter(rec -> rec.getRecordType().equals(recordType))
      .toList();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      List<Record> updated = original.stream()
        .map(RecordServiceTest::clone)
        .map(aRecord -> aRecord
          .withExternalIdsHolder(aRecord.getExternalIdsHolder().withInstanceId(UUID.randomUUID().toString())))
        .toList();
      recordCollection
        .withRecords(updated)
        .withTotalRecords(updated.size());
      List<ParsedRecord> expected = updated.stream()
        .map(Record::getParsedRecord)
        .toList();
      var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
      recordService.updateParsedRecords(recordCollection, okapiHeaders).onComplete(update -> {
        if (update.failed()) {
          testContext.failNow(update.cause());
          return;
        }

        ArgumentCaptor<Record> captureOldRecords = ArgumentCaptor.forClass(Record.class);
        ArgumentCaptor<Record> captureNewRecords = ArgumentCaptor.forClass(Record.class);

        verify(recordDomainEventPublisher, times(update.result().getTotalRecords()))
          .publishRecordUpdated(captureOldRecords.capture(), captureNewRecords.capture(), any());

        compareRecords(captureOldRecords.getAllValues(), original);
        compareRecords(captureNewRecords.getAllValues(), updated);

        assertEquals(0, update.result().getErrorMessages().size());
        assertEquals(expected.size(), update.result().getTotalRecords());
        compareParsedRecords(expected, update.result().getParsedRecords());
        Future.all(updated.stream().map(rec -> recordDao
          .getRecordByMatchedId(rec.getMatchedId(), TENANT_ID)
          .onComplete(get -> {
            if (get.failed()) {
              testContext.failNow(get.cause());
            }
            assertTrue(get.result().isPresent());
          })).toList()).onComplete(res -> {
          if (res.failed()) {
            testContext.failNow(res.cause());
            return;
          }
          testContext.completeNow();
        });
      });
    });
  }

  private void updateParsedMarcRecordsAndGetOnlyActualRecord(VertxTestContext testContext, Record expected) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      assertTrue(save.succeeded());
      expected.setLeaderRecordStatus("a");
      recordService.updateRecord(expected, okapiHeaders)
        .compose(v -> recordService.getFormattedRecord(expected.getMatchedId(), IdType.RECORD, TENANT_ID))
        .onComplete(get -> {
          assertTrue(get.succeeded());
          assertNotNull(get.result().getParsedRecord());
          assertEquals(expected.getParsedRecord().getFormattedContent(),
            get.result().getParsedRecord().getFormattedContent());
          assertEquals(get.result().getState().toString(), "ACTUAL");
          testContext.completeNow();
        });
    });
  }

  private void getFormattedMarcRecord(VertxTestContext testContext, Record expected) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      recordService
        .getFormattedRecord(expected.getMatchedId(), IdType.RECORD, TENANT_ID)
        .onComplete(get -> {
          if (get.failed()) {
            testContext.failNow(get.cause());
            return;
          }
          assertNotNull(get.result().getParsedRecord());
          assertEquals(expected.getParsedRecord().getFormattedContent(),
            get.result().getParsedRecord().getFormattedContent());
          testContext.completeNow();
        });
    });
  }

  private void updateSuppressFromDiscoveryForMarcRecord(VertxTestContext testContext, Record expected) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        testContext.failNow(save.cause());
        return;
      }
      recordService.updateSuppressFromDiscoveryForRecord(expected.getMatchedId(), IdType.RECORD, true, TENANT_ID)
        .onComplete(update -> {
          if (update.failed()) {
            testContext.failNow(update.cause());
            return;
          }
          assertTrue(update.result());
          recordDao.getRecordById(expected.getMatchedId(), TENANT_ID)
            .onComplete(get -> {
              if (get.failed()) {
                testContext.failNow(get.cause());
                return;
              }
              verify(recordDomainEventPublisher, times(0)).publishRecordUpdated(any(), any(), any());
              assertTrue(get.result().isPresent());
              assertNotNull(get.result().get().getRawRecord());
              assertNotNull(get.result().get().getParsedRecord());
              expected.setAdditionalInfo(expected.getAdditionalInfo().withSuppressDiscovery(true));
              compareRecords(expected, get.result().get());
              testContext.completeNow();
            });
        });
    });
  }

  private void deleteMarcRecordsBySnapshotId(VertxTestContext testContext, String snapshotId, RecordType parsedRecordType,
                                             Record.RecordType recordType) {
    List<Record> original = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        testContext.failNow(batch.cause());
        return;
      }
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordDao.getRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(getBefore -> {
        if (getBefore.failed()) {
          testContext.failNow(getBefore.cause());
          return;
        }
        int expected = (int) original.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .filter(rec -> rec.getSnapshotId().equals(snapshotId))
          .count();
        assertTrue(expected > 0);
        assertEquals(expected, getBefore.result().getTotalRecords());
        recordService.deleteRecordsBySnapshotId(snapshotId, TENANT_ID).onComplete(delete -> {
          if (delete.failed()) {
            testContext.failNow(delete.cause());
            return;
          }
          assertTrue(delete.result());
          recordDao.getRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(getAfter -> {
            if (getAfter.failed()) {
              testContext.failNow(getAfter.cause());
              return;
            }
            assertEquals(0, getAfter.result().getTotalRecords());
            SnapshotDaoUtil.findById(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshotId)
              .onComplete(getSnapshot -> {
                if (getSnapshot.failed()) {
                  testContext.failNow(getSnapshot.cause());
                  return;
                }
                assertFalse(getSnapshot.result().isPresent());
                testContext.completeNow();
              });
          });
        });
      });
    });
  }

  private CompositeFuture saveRecords(List<Record> records) {
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    return Future.all(records.stream()
      .map(rec -> recordService.saveRecord(rec, okapiHeaders))
      .toList()
    );
  }

  private void compareRecords(List<Record> expected, List<Record> actual) {
    assertEquals(expected.size(), actual.size());
    for (Record rec : expected) {
      var actualRecord = actual.stream()
        .filter(r -> Objects.equals(r.getId(), rec.getId()))
        .findFirst();
      actualRecord.ifPresent(value -> compareRecords(rec, value));
    }
  }

  private void compareRecords(Record expected, Record actual) {
    assertNotNull(actual);
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    assertEquals(expected.getMatchedId(), actual.getMatchedId());
    assertEquals(expected.getRecordType(), actual.getRecordType());
    assertEquals(expected.getState(), actual.getState());
    assertEquals(expected.getLeaderRecordStatus(), actual.getLeaderRecordStatus());
    assertEquals(expected.getOrder(), actual.getOrder());
    assertEquals(expected.getGeneration(), actual.getGeneration());
    if (Objects.nonNull(expected.getRawRecord())) {
      compareRawRecords(expected.getRawRecord(), actual.getRawRecord());
    } else {
      assertNull(actual.getRawRecord());
    }
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(expected.getParsedRecord(), actual.getParsedRecord());
    } else {
      assertNull(actual.getParsedRecord());
    }
    if (Objects.nonNull(expected.getErrorRecord())) {
      compareErrorRecords(expected.getErrorRecord(), actual.getErrorRecord());
    } else {
      assertNull(actual.getErrorRecord());
    }
    if (Objects.nonNull(expected.getAdditionalInfo())) {
      compareAdditionalInfo(expected.getAdditionalInfo(), actual.getAdditionalInfo());
    } else {
      assertNull(actual.getAdditionalInfo());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      compareExternalIdsHolder(expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    } else {
      assertNull(actual.getExternalIdsHolder());
    }
    if (Objects.nonNull(expected.getMetadata())) {
      compareMetadata(expected.getMetadata(), actual.getMetadata());
    } else {
      assertNull(actual.getMetadata());
    }
  }

  private void compareRecords(Record expected, StrippedParsedRecord actual) {
    assertNotNull(actual);
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getRecordType().toString(), actual.getRecordType().toString());
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(expected.getParsedRecord(), actual.getParsedRecord());
    } else {
      assertNull(actual.getParsedRecord());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      compareExternalIdsHolder(expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    } else {
      assertNull(actual.getExternalIdsHolder());
    }
  }

  private void compareSourceRecords(List<SourceRecord> expected, List<SourceRecord> actual) {
    assertEquals(expected.size(), actual.size());
    for (SourceRecord sourceRecord : expected) {
      var sourceRecordActual = actual.stream()
        .filter(sr -> Objects.equals(sr.getRecordId(), sourceRecord.getRecordId()))
        .findFirst();
      sourceRecordActual.ifPresent(rec -> compareSourceRecords(sourceRecord, rec));
    }
  }

  private void compareSourceRecords(SourceRecord expected, SourceRecord actual) {
    assertNotNull(actual);
    assertEquals(expected.getRecordId(), actual.getRecordId());
    assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    assertEquals(expected.getRecordType(), actual.getRecordType());
    assertEquals(expected.getOrder(), actual.getOrder());
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(expected.getParsedRecord(), actual.getParsedRecord());
    }
    if (Objects.nonNull(expected.getAdditionalInfo())) {
      compareAdditionalInfo(expected.getAdditionalInfo(), actual.getAdditionalInfo());
    } else {
      assertNull(actual.getAdditionalInfo());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      compareExternalIdsHolder(expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    } else {
      assertNull(actual.getExternalIdsHolder());
    }
    if (Objects.nonNull(expected.getMetadata())) {
      compareMetadata(expected.getMetadata(), actual.getMetadata());
    } else {
      assertNull(actual.getMetadata());
    }
  }

  private void compareParsedRecords(List<ParsedRecord> expected, List<ParsedRecord> actual) {
    assertEquals(expected.size(), actual.size());
    for (ParsedRecord parsedRecord : expected) {
      var actualParsedRecord = actual.stream().filter(a -> Objects.equals(a.getId(), parsedRecord.getId())).findFirst();
      actualParsedRecord.ifPresent(rec -> compareParsedRecords(parsedRecord, rec));
    }
  }

  private void compareRawRecords(RawRecord expected, RawRecord actual) {
    assertNotNull(actual);
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getContent(), actual.getContent());
  }

  private void compareParsedRecords(ParsedRecord expected, ParsedRecord actual) {
    assertNotNull(actual);
    assertEquals(expected.getId(), actual.getId());
    assertEquals(ParsedRecordDaoUtil.normalizeContent(expected), ParsedRecordDaoUtil.normalizeContent(actual));
  }

  private void compareErrorRecords(ErrorRecord expected, ErrorRecord actual) {
    assertNotNull(actual);
    assertEquals(expected.getId(), actual.getId());
    assertEquals(expected.getContent(), actual.getContent());
    assertEquals(expected.getDescription(), actual.getDescription());
  }

  private void compareAdditionalInfo(AdditionalInfo expected, AdditionalInfo actual) {
    assertEquals(expected.getSuppressDiscovery(), actual.getSuppressDiscovery());
  }

  private void compareExternalIdsHolder(ExternalIdsHolder expected, ExternalIdsHolder actual) {
    assertEquals(expected.getInstanceId(), actual.getInstanceId());
  }

  private static <T> T clone(T obj) {
    try {
      final ObjectMapper jsonMapper = ObjectMapperTool.getMapper();
      return jsonMapper.readValue(jsonMapper.writeValueAsString(obj), (Class<T>) Record.class);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException(ex);
    }
  }
}
