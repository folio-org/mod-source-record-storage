package org.folio.services;

import static java.util.Comparator.comparing;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.services.RecordServiceImpl.INDICATOR;
import static org.folio.services.RecordServiceImpl.SUBFIELD_S;
import static org.folio.services.RecordServiceImpl.UPDATE_RECORD_WITH_LINKED_DATA_ID_EXCEPTION;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.AdditionalFieldsUtil.getFieldFromMarcRecord;
import static org.junit.Assert.assertThrows;
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
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.Conditions;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.FetchParsedRecordsBatchRequest;
import org.folio.rest.jaxrs.model.FieldRange;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.StrippedParsedRecord;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.exceptions.RecordUpdateException;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class RecordServiceTest extends AbstractLBServiceTest {

  private static final String MARC_BIB_RECORD_SNAPSHOT_ID = "d787a937-cc4b-49b3-85ef-35bcd643c689";
  private static final String MARC_AUTHORITY_RECORD_SNAPSHOT_ID = "ee561342-3098-47a8-ab6e-0f3eba120b04";
  private static final String HR_ID = "inst00007";

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;

  private RecordService recordService;

  private static RawRecord rawRecord;
  private static ParsedRecord marcRecord;

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.openMocks(this);
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    marcRecord = new ParsedRecord()
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    recordService = new RecordServiceImpl(recordDao);
    Async async = context.async();
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots()).onComplete(save -> {
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
  public void shouldGetMarcBibRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.MARC_BIB, orderFields, 1, 2, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .sorted(comparing(Record::getOrder))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.get(1), get.result().getRecords().get(0));
        compareRecords(context, expected.get(2), get.result().getRecords().get(1));
        async.complete();
      });
    });
  }

  @Test
  public void shouldFetchBibRecordsWithFieldsRangeByExternalId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }

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
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> r.getExternalIdsHolder().getInstanceId().equals(externalId))
          .collect(Collectors.toList());
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.getFirst(), get.result().getRecords().getFirst());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFetchBibRecordsWithOneFieldByExternalId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }

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
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> r.getExternalIdsHolder().getInstanceId().equals(externalId))
          .peek(r -> r.getParsedRecord().setContent(expectedContent))
          .collect(Collectors.toList());
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.getFirst(), get.result().getRecords().getFirst());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFetchActualAndDeletedBibRecordsWithOneFieldByExternalIdWhenIncludeDeletedExists(TestContext context) {
    Async async = context.async();
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
        context.fail(batch.cause());
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
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> externalIds.contains(r.getExternalIdsHolder().getInstanceId()))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFetchActualBibRecordsWithOneFieldByExternalIdWhenIncludeDeletedNotExists(TestContext context) {
    Async async = context.async();
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
        context.fail(batch.cause());
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
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> externalIds.contains(r.getExternalIdsHolder().getInstanceId()))
          .filter(r -> r.getState().equals(State.ACTUAL))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFetchActualAndDeletedBibRecordsWithOneFieldByExternalIdWhenIncludeDeletedFalse(TestContext context) {
    Async async = context.async();
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
        context.fail(batch.cause());
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
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .filter(r -> externalIds.contains(r.getExternalIdsHolder().getInstanceId()))
          .filter(r -> r.getState().equals(State.ACTUAL))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetMarcAuthorityRecordsBySnapshotId(TestContext context) {
    getRecordsBySnapshotId(context, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_AUTHORITY,
      Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldGetMarcHoldingsRecordsBySnapshotId(TestContext context) {
    getRecordsBySnapshotId(context, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_HOLDING,
      Record.RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldGetEdifactRecordsBySnapshotId(TestContext context) {
    getRecordsBySnapshotId(context, "dcd898af-03bb-4b12-b8a6-f6a02e86459b", RecordType.EDIFACT, Record.RecordType.EDIFACT);
  }

  @Test
  public void shouldStreamMarcBibRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
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

          context.assertEquals(expected.size(), actual.size());
          compareRecords(context, expected.get(0), actual.get(0));
          compareRecords(context, expected.get(1), actual.get(1));
          compareRecords(context, expected.get(2), actual.get(2));

          async.complete();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  @Test
  public void shouldStreamMarcAuthorityRecordsBySnapshotId(TestContext context) {
    streamRecordsBySnapshotId(context, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_AUTHORITY,
      Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldStreamMarcHoldingsRecordsBySnapshotId(TestContext context) {
    streamRecordsBySnapshotId(context, "ee561342-3098-47a8-ab6e-0f3eba120b04", RecordType.MARC_HOLDING,
      Record.RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldStreamEdifactRecordsBySnapshotId(TestContext context) {
    streamRecordsBySnapshotId(context, "dcd898af-03bb-4b12-b8a6-f6a02e86459b", RecordType.EDIFACT,
      Record.RecordType.EDIFACT);
  }

  @Test
  public void shouldGetMarcRecordsBetweenDates(TestContext context) {
    getMarcRecordsBetweenDates(context, OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS),
      OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS).plusDays(1), RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldGetMarcBibRecordById(TestContext context) {
    getMarcRecordById(context, TestMocks.getMarcBibRecord());
  }

  @Test
  public void shouldGetMarcAuthorityRecordById(TestContext context) {
    getMarcRecordById(context, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  public void shouldGetMarcHoldingsRecordById(TestContext context) {
    getMarcRecordById(context, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  public void shouldNotGetRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldSaveMarcBibRecord(TestContext context) {
    saveMarcRecord(context, TestMocks.getMarcBibRecord(), Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldSaveMarcBibRecordWithMatchedIdFrom999field(TestContext context) {
    String marc999 = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();
    ParsedRecord parsedRecord = new ParsedRecord().withId(marc999)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", marc999)))
          .put("ind1", "f")
          .put("ind2", "f"))).add(new JsonObject().put("001", HR_ID))).encode());
    Record record = new Record()
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
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(record, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertNotNull(save.result().getRawRecord());
      context.assertNotNull(save.result().getParsedRecord());
      compareRecords(context, record, save.result());
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertEquals(marc999, get.result().get().getMatchedId());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFailDuringUpdateRecordGenerationIfIncomingMatchedIdNotEqualToMatchedIdFrom999field(TestContext context) {
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
    Record record = new Record()
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
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.updateRecordGeneration(matchedId, record, okapiHeaders).onComplete(save -> {
      context.assertTrue(save.failed());
      context.assertTrue(save.cause() instanceof BadRequestException);
      recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isEmpty());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFailDuringUpdateRecordGenerationIfRecordWithIdAsIncomingMatchedIfNotExist(TestContext context) {
    String matchedId = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();
    ParsedRecord parsedRecord = new ParsedRecord().withId(matchedId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", matchedId)))
          .put("ind1", "f")
          .put("ind2", "f")))).encode());
    Record record = new Record()
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
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.updateRecordGeneration(matchedId, record, okapiHeaders).onComplete(save -> {
      context.assertTrue(save.failed());
      context.assertTrue(save.cause() instanceof NotFoundException);
      recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isEmpty());
        async.complete();
      });
    });
  }

  @Test
  public void shouldFailUpdateRecordGenerationIfDuplicateError(TestContext context) {
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
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(record1, okapiHeaders).onComplete(record1Saved -> {
      if (record1Saved.failed()) {
        context.fail(record1Saved.cause());
      }
      context.assertNotNull(record1Saved.result().getRawRecord());
      context.assertNotNull(record1Saved.result().getParsedRecord());
      context.assertEquals(record1Saved.result().getState(), State.ACTUAL);
      compareRecords(context, record1, record1Saved.result());

      SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(snapshotSaved -> {
        if (snapshotSaved.failed()) {
          context.fail(snapshotSaved.cause());
        }
        recordService.updateRecordGeneration(matchedId, recordToUpdateGeneration, okapiHeaders).onComplete(recordToUpdateGenerationSaved -> {
          context.assertTrue(recordToUpdateGenerationSaved.failed());
          context.assertTrue(recordToUpdateGenerationSaved.cause() instanceof BadRequestException);
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldUpdateRecordGeneration(TestContext context) {
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
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(record1, okapiHeaders).onComplete(record1Saved -> {
      if (record1Saved.failed()) {
        context.fail(record1Saved.cause());
      }
      context.assertNotNull(record1Saved.result().getRawRecord());
      context.assertNotNull(record1Saved.result().getParsedRecord());
      context.assertEquals(record1Saved.result().getState(), State.ACTUAL);
      compareRecords(context, record1, record1Saved.result());

      SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot).onComplete(snapshotSaved -> {
        if (snapshotSaved.failed()) {
          context.fail(snapshotSaved.cause());
        }
        recordService.updateRecordGeneration(matchedId, recordToUpdateGeneration, okapiHeaders).onComplete(recordToUpdateGenerationSaved -> {
          verify(recordDomainEventPublisher).publishRecordUpdated(eq(record1Saved.result()), eq(recordToUpdateGenerationSaved.result()), any());
          context.assertTrue(recordToUpdateGenerationSaved.succeeded());
          context.assertEquals(recordToUpdateGenerationSaved.result().getMatchedId(), matchedId);
          context.assertEquals(recordToUpdateGenerationSaved.result().getGeneration(), 1);
          recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
            if (get.failed()) {
              context.fail(get.cause());
            }
            context.assertTrue(get.result().isPresent());
            context.assertEquals(get.result().get().getGeneration(), 1);
            context.assertEquals(get.result().get().getMatchedId(), matchedId);
            context.assertNotEquals(get.result().get().getId(), matchedId);
            context.assertEquals(get.result().get().getState(), State.ACTUAL);
            recordDao.getRecordById(matchedId, TENANT_ID).onComplete(getRecord1 -> {
              if (getRecord1.failed()) {
                context.fail(get.cause());
              }
              context.assertTrue(getRecord1.result().isPresent());
              context.assertEquals(getRecord1.result().get().getState(), State.OLD);
              async.complete();
            });
          });
        });
      });
    });
  }

  @Test
  public void shouldUpdateRecordGenerationByMatchId(TestContext context) {
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

    var async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(recordToSave, okapiHeaders).onComplete(savedRecord -> {
      if (savedRecord.failed()) {
        context.fail(savedRecord.cause());
      }
      context.assertNotNull(savedRecord.result().getRawRecord());
      context.assertNotNull(savedRecord.result().getParsedRecord());
      context.assertEquals(savedRecord.result().getState(), State.ACTUAL);
      compareRecords(context, recordToSave, savedRecord.result());

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
          context.fail(snapshotSaved.cause());
        }

        recordService.updateRecordGeneration(matchedId, recordToUpdateGeneration, okapiHeaders).onComplete(recordToUpdateGenerationSaved -> {
          context.assertTrue(recordToUpdateGenerationSaved.succeeded());
          context.assertEquals(recordToUpdateGenerationSaved.result().getMatchedId(), matchedId);
          context.assertEquals(recordToUpdateGenerationSaved.result().getGeneration(), 1);
          recordDao.getRecordByMatchedId(matchedId, TENANT_ID).onComplete(get -> {
            if (get.failed()) {
              context.fail(get.cause());
            }
            context.assertTrue(get.result().isPresent());
            context.assertEquals(get.result().get().getGeneration(), 1);
            context.assertEquals(get.result().get().getMatchedId(), matchedId);
            context.assertNotEquals(get.result().get().getId(), matchedId);
            context.assertEquals(get.result().get().getState(), State.ACTUAL);
            recordDao.getRecordById(matchedId, TENANT_ID).onComplete(getRecord1 -> {
              if (getRecord1.failed()) {
                context.fail(get.cause());
              }
              context.assertTrue(getRecord1.result().isPresent());
              context.assertEquals(getRecord1.result().get().getState(), State.OLD);
              async.complete();
            });
          });
        });
      });
    });
  }

  @Test
  public void shouldSaveMarcBibRecordWithMatchedIdFromRecordId(TestContext context) {
    Record original = TestMocks.getMarcBibRecord();
    String recordId = UUID.randomUUID().toString();

    Record record = new Record()
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
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(record, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertNotNull(save.result().getRawRecord());
      context.assertNotNull(save.result().getParsedRecord());
      compareRecords(context, record, save.result());
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertEquals(recordId, get.result().get().getMatchedId());
        context.assertEquals(getFieldFromMarcRecord(get.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S), recordId);
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveEdifactRecordAndNotSet999Field(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecords(Record.RecordType.EDIFACT);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(record, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordDao.getRecordById(record.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertEquals(record.getId(), get.result().get().getMatchedId());
        context.assertNull(getFieldFromMarcRecord(get.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S));
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveMarcBibRecordWithMatchedIdFromExistingSourceRecord(TestContext context) {
    Async async = context.async();
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
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(record1, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertNotNull(save.result().getRawRecord());
      context.assertNotNull(save.result().getParsedRecord());
      compareRecords(context, record1, save.result());
      recordDao.getRecordById(record1.getId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertEquals(recordId1, get.result().get().getMatchedId());
        context.assertEquals(getFieldFromMarcRecord(get.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S), recordId1);

        recordService.saveRecord(record2, okapiHeaders).onComplete(save2 -> {
          if (save2.failed()) {
            context.fail(save2.cause());
          }
          context.assertNotNull(save2.result().getRawRecord());
          context.assertNotNull(save2.result().getParsedRecord());
          compareRecords(context, record2, save2.result());
          recordDao.getRecordById(record2.getId(), TENANT_ID).onComplete(get2 -> {
            if (get2.failed()) {
              context.fail(get2.cause());
            }
            context.assertTrue(get2.result().isPresent());
            context.assertNotNull(get2.result().get().getRawRecord());
            context.assertNotNull(get2.result().get().getParsedRecord());
            context.assertEquals(recordId1, get2.result().get().getMatchedId());
            context.assertEquals(getFieldFromMarcRecord(get2.result().get(), TAG_999, INDICATOR, INDICATOR, SUBFIELD_S), recordId1);
            async.complete();
          });
        });
      });
    });
  }

  @Test
  public void shouldSaveMarcAuthorityRecord(TestContext context) {
    saveMarcRecord(context, TestMocks.getMarcAuthorityRecord(), Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldSaveMarcHoldingsRecord(TestContext context) {
    saveMarcRecord(context, TestMocks.getMarcHoldingsRecord(), Record.RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldSaveEdifactRecord(TestContext context) {
    saveMarcRecord(context, TestMocks.getEdifactRecord(), Record.RecordType.EDIFACT);
  }

  @Test
  public void shouldSaveMarcBibRecordWithGenerationGreaterThanZero(TestContext context) {
    saveMarcRecordWithGenerationGreaterThanZero(context, TestMocks.getMarcBibRecord(), Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldFailToSaveRecord(TestContext context) {
    Async async = context.async();
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
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordService.saveRecord(invalid, okapiHeaders).onComplete(save -> {
      context.assertTrue(save.failed());
      String expected = "Invalid UUID string: " + fakeSnapshotId;
      context.assertTrue(save.cause().getMessage().contains(expected));
      async.complete();
    });
  }

  @Test
  public void shouldSaveMarcBibRecords(TestContext context) {
    saveMarcRecords(context, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldSaveMarcAuthorityRecords(TestContext context) {
    saveMarcRecords(context, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldSaveEdifactRecords(TestContext context) {
    saveMarcRecords(context, Record.RecordType.EDIFACT);
  }

  @Test
  public void shouldSaveMarcBibRecordsWithExpectedErrors(TestContext context) {
    saveMarcRecordsWithExpectedErrors(context, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldUpdateMarcRecord(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(original, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
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
          context.fail(update.cause());
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordUpdated(eq(save.result()), eq(update.result()), any());
        context.assertTrue(update.result().getMetadata().getUpdatedDate()
          .after(update.result().getMetadata().getCreatedDate()));
        context.assertNotNull(update.result().getRawRecord());
        context.assertNotNull(update.result().getParsedRecord());
        context.assertNull(update.result().getErrorRecord());
        compareRecords(context, expected, update.result());
        Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(expected.getMatchedId()))
          .and(RECORDS_LB.STATE.eq(RecordState.OLD));
        recordDao.getRecordByCondition(condition, TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldUpdateParsedRecord(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(original, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
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
          context.fail(update.cause());
        }
        recordService.getRecordById(expected.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());

          ArgumentCaptor<Record> captureOldRecord = ArgumentCaptor.forClass(Record.class);
          ArgumentCaptor<Record> captureNewRecord = ArgumentCaptor.forClass(Record.class);
          Record expectedNewRecord = MarcUtil.clone(get.result().get(), Record.class).withErrorRecord(null).withRawRecord(null);

          verify(recordDomainEventPublisher, times(1))
            .publishRecordUpdated(captureOldRecord.capture(), captureNewRecord.capture(), any());

          compareRecords(context, captureOldRecord.getValue(), save.result());
          compareRecords(context, captureNewRecord.getValue(), expectedNewRecord);
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldUpdateRecordState(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getMarcBibRecord();
    String snapshotId = UUID.randomUUID().toString();
    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(original.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(original.getRecordType().toString()))
      .withParsedRecord(original.getParsedRecord())
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(original.getExternalIdsHolder())
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders))
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders))
      .compose(ar -> recordService.updateRecordsState(original.getMatchedId(), RecordState.DRAFT, RecordType.MARC_BIB, TENANT_ID))
      .onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(original.getMatchedId()));
        recordService.getRecords(condition, RecordType.MARC_BIB, Collections.emptyList(), 0, 999, TENANT_ID)
          .onComplete(get -> {
            if (get.failed()) {
              context.fail(get.cause());
            }
            List<Record> resultRecords = get.result().getRecords();
            context.assertFalse(resultRecords.isEmpty());
            context.assertEquals(3, resultRecords.size());
            context.assertTrue(resultRecords.stream().allMatch(record -> record.getState() == State.DRAFT));
            async.complete();
          });
      });
  }

  @Test
  public void shouldUpdateMarcAuthorityRecordStateToDeleted(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getMarcAuthorityRecord();
    String snapshotId = UUID.randomUUID().toString();
    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(original.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(original.getRecordType().toString()))
      .withParsedRecord(original.getParsedRecord())
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(original.getExternalIdsHolder())
      .withMetadata(original.getMetadata());
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders))
      .compose(ar -> recordService.updateRecordsState(original.getMatchedId(), RecordState.DELETED, RecordType.MARC_AUTHORITY, TENANT_ID))
      .onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(original.getMatchedId()));
        recordService.getRecords(condition, RecordType.MARC_AUTHORITY, Collections.emptyList(), 0, 999, TENANT_ID)
          .onComplete(get -> {
            if (get.failed()) {
              context.fail(get.cause());
            }
            List<Record> resultRecords = get.result().getRecords();
            context.assertFalse(resultRecords.isEmpty());
            context.assertEquals(2, resultRecords.size());
            context.assertTrue(resultRecords.stream().allMatch(record -> record.getState() == State.DELETED));
            context.assertTrue(resultRecords.stream().allMatch(record -> "d".equals(record.getLeaderRecordStatus())));
            async.complete();
          });
      });
  }

  @Test
  public void shouldUpdateEdifactRecord(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getEdifactRecord();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
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
          context.fail(update.cause());
        }
        context.assertTrue(update.result().getMetadata().getUpdatedDate()
          .after(update.result().getMetadata().getCreatedDate()));
        context.assertNotNull(update.result().getRawRecord());
        context.assertNotNull(update.result().getParsedRecord());
        context.assertNull(update.result().getErrorRecord());
        compareRecords(context, expected, update.result());
        Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(expected.getMatchedId()))
          .and(RECORDS_LB.STATE.eq(RecordState.OLD));
        recordDao.getRecordByCondition(condition, TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldFailToUpdateRecord(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.getRecordById(record.getMatchedId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
      recordService.updateRecord(record, okapiHeaders).onComplete(update -> {
        context.assertTrue(update.failed());
        String expected = String.format("Record with id '%s' was not found", record.getId());
        context.assertEquals(expected, update.cause().getMessage());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetMarcBibSourceRecords(TestContext context) {
    getMarcSourceRecords(context, RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldGetMarcAuthoritySourceRecords(TestContext context) {
    getMarcSourceRecords(context, RecordType.MARC_AUTHORITY, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldGetEdifactSourceRecords(TestContext context) {
    getMarcSourceRecords(context, RecordType.EDIFACT, Record.RecordType.EDIFACT);
  }

  @Test
  public void shouldStreamMarcBibSourceRecords(TestContext context) {
    streamMarcSourceRecords(context, RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldStreamMarcAuthoritySourceRecords(TestContext context) {
    streamMarcSourceRecords(context, RecordType.MARC_AUTHORITY, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldStreamMarcHoldingSourceRecords(TestContext context) {
    streamMarcSourceRecords(context, RecordType.MARC_HOLDING, Record.RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldStreamEdifactSourceRecords(TestContext context) {
    streamMarcSourceRecords(context, RecordType.EDIFACT, Record.RecordType.EDIFACT);
  }

  @Test
  public void shouldGetMarcBibSourceRecordsByListOfIds(TestContext context) {
    getMarcSourceRecordsByListOfIds(context, Record.RecordType.MARC_BIB, RecordType.MARC_BIB);
  }

  @Test
  public void shouldGetMarcAuthoritySourceRecordsByListOfIds(TestContext context) {
    getMarcSourceRecordsByListOfIds(context, Record.RecordType.MARC_AUTHORITY, RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldGetMarcHoldingsSourceRecordsByListOfIds(TestContext context) {
    getMarcSourceRecordsByListOfIds(context, Record.RecordType.MARC_HOLDING, RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldGetMarcBibSourceRecordsByListOfIdsThatAreDeleted(TestContext context) {
    getMarcSourceRecordsByListOfIdsThatAreDeleted(context, Record.RecordType.MARC_BIB, RecordType.MARC_BIB);
  }

  @Test
  public void shouldGetMarcAuthoritySourceRecordsByListOfIdsThatAreDeleted(TestContext context) {
    getMarcSourceRecordsByListOfIdsThatAreDeleted(context, Record.RecordType.MARC_AUTHORITY, RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldGetMarcHoldingsSourceRecordsByListOfIdsThatAreDeleted(TestContext context) {
    getMarcSourceRecordsByListOfIdsThatAreDeleted(context, Record.RecordType.MARC_HOLDING, RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldGetMarcBibSourceRecordsBetweenDates(TestContext context) {
    getMarcSourceRecordsBetweenDates(context, Record.RecordType.MARC_BIB, RecordType.MARC_BIB,
      OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS), OffsetDateTime.now().truncatedTo(ChronoUnit.DAYS).plusDays(1));
  }

  @Test
  public void shouldGetMarcBibSourceRecordById(TestContext context) {
    getMarcSourceRecordById(context, TestMocks.getMarcBibRecord());
  }

  @Test
  public void shouldGetMarcAuthoritySourceRecordById(TestContext context) {
    getMarcSourceRecordById(context, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  public void shouldGetMarcHoldingsSourceRecordById(TestContext context) {
    getMarcSourceRecordById(context, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  public void shouldGetMarcBibSourceRecordByMatchedIdNotEqualToId(TestContext context) {
    Record expected = TestMocks.getMarcBibRecord();
    Async async = context.async();
    String snapshotId = UUID.randomUUID().toString();
    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(expected.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(expected.getRecordType().toString()))
      .withParsedRecord(expected.getParsedRecord())
      .withAdditionalInfo(expected.getAdditionalInfo())
      .withExternalIdsHolder(expected.getExternalIdsHolder())
      .withMetadata(expected.getMetadata());
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders))
      .onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        recordDao
          .getRecordByMatchedId(expected.getMatchedId(), TENANT_ID)
          .onComplete(get -> {
            if (get.failed()) {
              context.fail(get.cause());
            }
            context.assertTrue(get.result().isPresent());
            context.assertNotNull(get.result().get().getRawRecord());
            context.assertNotNull(get.result().get().getParsedRecord());
            context.assertEquals(expected.getMatchedId(), get.result().get().getMatchedId());
            context.assertTrue(get.result().get().getGeneration() > 0);
            context.assertNotEquals(get.result().get().getMatchedId(), get.result().get().getId());
            async.complete();
          });
      });
  }

  @Test
  public void shouldNotGetMarcBibSourceRecordById(TestContext context) {
    notGetMarcSourceRecordById(context, TestMocks.getMarcBibRecord());
  }

  @Test
  public void shouldNotGetMarcAuthoritySourceRecordById(TestContext context) {
    notGetMarcSourceRecordById(context, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  public void shouldNotGetMarcHoldingsSourceRecordById(TestContext context) {
    notGetMarcSourceRecordById(context, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  public void shouldUpdateParsedMarcBibRecords(TestContext context) {
    updateParsedMarcRecords(context, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldUpdateParsedMarcAuthorityRecords(TestContext context) {
    updateParsedMarcRecords(context, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldUpdateParsedMarcHoldingsRecords(TestContext context) {
    updateParsedMarcRecords(context, Record.RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldUpdateParsedMarcBibRecordsAndGetOnlyActualRecord(TestContext context) {
    updateParsedMarcRecordsAndGetOnlyActualRecord(context, TestMocks.getMarcBibRecord());
  }

  @Test
  public void shouldUpdateParsedMarcAuthorityRecordsAndGetOnlyActualRecord(TestContext context) {
    updateParsedMarcRecordsAndGetOnlyActualRecord(context, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  public void shouldUpdateParsedMarcHoldingsRecordsAndGetOnlyActualRecord(TestContext context) {
    updateParsedMarcRecordsAndGetOnlyActualRecord(context, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  public void shouldGetFormattedMarcBibRecord(TestContext context) {
    getFormattedMarcRecord(context, TestMocks.getMarcBibRecord());
  }

  @Test
  public void shouldGetFormattedMarcAuthorityRecord(TestContext context) {
    getFormattedMarcRecord(context, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  public void shouldGetFormattedMarcHoldingsRecord(TestContext context) {
    getFormattedMarcRecord(context, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  public void shouldGetFormattedEdifactRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getEdifactRecord();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getFormattedRecord(expected.getId(), IdType.RECORD, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertNotNull(get.result().getParsedRecord());
        context.assertEquals(expected.getParsedRecord().getFormattedContent(),
          get.result().getParsedRecord().getFormattedContent());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetFormattedDeletedRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getMarcBibRecord();
    expected.setState(State.DELETED);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getFormattedRecord(expected.getId(), IdType.RECORD, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertNotNull(get.result().getParsedRecord());
        context.assertEquals(expected.getParsedRecord().getFormattedContent(),
          get.result().getParsedRecord().getFormattedContent());
        async.complete();
      });
    });
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForMarcBibRecord(TestContext context) {
    updateSuppressFromDiscoveryForMarcRecord(context, TestMocks.getMarcBibRecord());
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForMarcAuthorityRecord(TestContext context) {
    updateSuppressFromDiscoveryForMarcRecord(context, TestMocks.getMarcAuthorityRecord());
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForMarcHoldingsRecord(TestContext context) {
    updateSuppressFromDiscoveryForMarcRecord(context, TestMocks.getMarcHoldingsRecord());
  }

  @Test
  public void shouldDeleteMarcBibRecordsBySnapshotId(TestContext context) {
    deleteMarcRecordsBySnapshotId(context, MARC_BIB_RECORD_SNAPSHOT_ID, RecordType.MARC_BIB, Record.RecordType.MARC_BIB);
  }

  @Test
  public void shouldDeleteMarcAuthorityRecordsBySnapshotId(TestContext context) {
    deleteMarcRecordsBySnapshotId(context, MARC_AUTHORITY_RECORD_SNAPSHOT_ID, RecordType.MARC_AUTHORITY, Record.RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldUpdateSourceRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      String snapshotId = UUID.randomUUID().toString();
      ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
        .withId(expected.getId())
        .withRecordType(ParsedRecordDto.RecordType.fromValue(expected.getRecordType().toString()))
        .withParsedRecord(expected.getParsedRecord())
        .withAdditionalInfo(expected.getAdditionalInfo())
        .withExternalIdsHolder(expected.getExternalIdsHolder())
        .withMetadata(expected.getMetadata());
      recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordUpdated(eq(save.result()), eq(update.result()), any());
        SnapshotDaoUtil.findById(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshotId).onComplete(getSnapshot -> {
          if (getSnapshot.failed()) {
            context.fail(getSnapshot.cause());
          }
          context.assertTrue(getSnapshot.result().isPresent());
          context.assertNotNull(getSnapshot.result().get().getProcessingStartedDate());
          recordDao.getRecordByCondition(RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId)), TENANT_ID)
            .onComplete(getNewRecord -> {
              if (getNewRecord.failed()) {
                context.fail(getNewRecord.cause());
              }
              context.assertTrue(getNewRecord.result().isPresent());
              context.assertEquals(State.ACTUAL, getNewRecord.result().get().getState());
              context.assertFalse(getNewRecord.result().get().getAdditionalInfo().getSuppressDiscovery());
              context.assertFalse(getNewRecord.result().get().getDeleted());
              context.assertEquals(expected.getGeneration() + 1, getNewRecord.result().get().getGeneration());
              recordDao.getRecordByCondition(RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(expected.getSnapshotId())), TENANT_ID)
                .onComplete(getOldRecord -> {
                  if (getOldRecord.failed()) {
                    context.fail(getOldRecord.cause());
                  }
                  context.assertTrue(getOldRecord.result().isPresent());
                  context.assertEquals(State.OLD, getOldRecord.result().get().getState());
                  async.complete();
                });
            });
        });
      });
    });
  }

  @Test
  public void shouldUpdateRecordStateToDeletedWhenLeaderIsDeleted(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getMarcBibRecord();
    String snapshotId = UUID.randomUUID().toString();
    ParsedRecord parsedRecord = new ParsedRecord()
      .withId(original.getId())
      .withContent(new JsonObject().put("leader", "01542dcm a2200361   4500").put("fields", new JsonArray()));

    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(original.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(original.getRecordType().toString()))
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(original.getExternalIdsHolder())
      .withMetadata(original.getMetadata());

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders))
      .onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertTrue(update.result().getDeleted());
        context.assertEquals(State.DELETED, update.result().getState());
        context.assertTrue(update.result().getAdditionalInfo().getSuppressDiscovery());
        recordDao.getRecordById(original.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());
          context.assertEquals(State.OLD, get.result().get().getState());
          async.complete();
        });
      });
  }

  @Test
  public void shouldUpdateRecordStateToDeletedWhenLeaderIsDeletedAndAdditionalInfoIsNull(TestContext context) {
    Async async = context.async();
    Record original = TestMocks.getMarcBibRecord();
    String snapshotId = UUID.randomUUID().toString();
    ParsedRecord parsedRecord = new ParsedRecord()
      .withId(original.getId())
      .withContent(new JsonObject().put("leader", "01542dcm a2200361   4500").put("fields", new JsonArray()));

    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(original.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(original.getRecordType().toString()))
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(original.getExternalIdsHolder())
      .withMetadata(original.getMetadata());

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders))
      .onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertTrue(update.result().getDeleted());
        context.assertEquals(State.DELETED, update.result().getState());
        context.assertTrue(update.result().getAdditionalInfo().getSuppressDiscovery());
        recordDao.getRecordById(original.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());
          context.assertEquals(State.OLD, get.result().get().getState());
          async.complete();
        });
      });
  }

  @Test
  public void shouldUpdateRecordStateToActualWhenLeaderChangedFromDeleted(TestContext context) {
    Async async = context.async();
    String snapshotId = UUID.randomUUID().toString();
    Record original = TestMocks.getMarcBibRecord();
    original.setState(State.DELETED);
    original.setDeleted(true);
    original.setAdditionalInfo(original.getAdditionalInfo().withSuppressDiscovery(true));
    original.setParsedRecord(new ParsedRecord()
      .withId(original.getId())
      .withContent(new JsonObject().put("leader", "01542dcm a2200361   4500").put("fields", new JsonArray())));

    ParsedRecord parsedRecord = new ParsedRecord()
      .withId(original.getId())
      .withContent(new JsonObject().put("leader", "01542cam a2200361   4500").put("fields", new JsonArray()));

    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(original.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(original.getRecordType().toString()))
      .withParsedRecord(parsedRecord)
      .withAdditionalInfo(original.getAdditionalInfo())
      .withExternalIdsHolder(original.getExternalIdsHolder())
      .withMetadata(original.getMetadata());

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(original, okapiHeaders)
      .compose(ar -> {
        context.assertTrue(ar.getDeleted());
        context.assertEquals(State.DELETED, ar.getState());
        context.assertTrue(ar.getAdditionalInfo().getSuppressDiscovery());
        return recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders);
      })
      .onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertFalse(update.result().getDeleted());
        context.assertEquals(State.ACTUAL, update.result().getState());
        context.assertTrue(update.result().getAdditionalInfo().getSuppressDiscovery());
        recordDao.getRecordById(original.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());
          context.assertEquals(State.OLD, get.result().get().getState());
          async.complete();
        });
      });
  }

  @Test
  public void shouldGetNoRecordsWithLimitEqualsZero(TestContext context) {
    getTotalRecordsAndRecordsDependsOnLimit(context, 0);
  }

  @Test
  public void shouldGetNoRecordsWithLimitNotEqualsZero(TestContext context) {
    getTotalRecordsAndRecordsDependsOnLimit(context, 1);
  }

  @Test
  public void shouldThrowExceptionWhenSavedDuplicateRecord(TestContext context) {
    Async async = context.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(Record.RecordType.MARC_BIB))
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    List<Future<RecordsBatchResponse>> futures = List.of(recordService.saveRecords(recordCollection, okapiHeaders),
      recordService.saveRecords(recordCollection, okapiHeaders));

    GenericCompositeFuture.all(futures).onComplete(ar -> {
      context.assertTrue(ar.failed());
      assertThrows(DuplicateEventException.class, () -> {throw ar.cause();});
      async.complete();
    });
  }

  @Test
  public void shouldThrowExceptionWhenUpdatingLinkedDataRecord(TestContext context) {
    Async async = context.async();
    Record sourceRecord = TestMocks.getMarcBibRecord();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    ParsedRecord parsedRecord = new ParsedRecord().withId(sourceRecord.getId())
            .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
                    .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
                            .put("subfields",
                                    new JsonArray().add(new JsonObject().put("s", sourceRecord.getId()))
                                            .add(new JsonObject().put("l", "503ac913-4e7e-4943-b728-a42843579132")))
                            .put("ind1", "f")
                            .put("ind2", "f")))).encode());

    recordDao.saveRecord(sourceRecord.withParsedRecord(parsedRecord), okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      String snapshotId = UUID.randomUUID().toString();
      ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
              .withId(sourceRecord.getId())
              .withRecordType(ParsedRecordDto.RecordType.fromValue(sourceRecord.getRecordType().toString()))
              .withParsedRecord(sourceRecord.getParsedRecord())
              .withAdditionalInfo(sourceRecord.getAdditionalInfo())
              .withExternalIdsHolder(sourceRecord.getExternalIdsHolder())
              .withMetadata(sourceRecord.getMetadata());

      recordService.updateSourceRecord(parsedRecordDto, snapshotId, okapiHeaders).onComplete(ar -> {
        context.assertTrue(ar.failed());
        context.assertEquals(UPDATE_RECORD_WITH_LINKED_DATA_ID_EXCEPTION, ar.cause().getMessage());
        assertThrows(RecordUpdateException.class, () -> {
          throw ar.cause();
        });
        async.complete();
      });
    });
  }

  @Test
  public void shouldHardDeleteMarcRecord(TestContext context) {
    Async async = context.async();
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

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(sourceRecord, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }

      recordService.deleteRecordsByExternalId(sourceRecord.getExternalIdsHolder().getInstanceId(), okapiHeaders).onComplete(delete -> {
        if (delete.failed()) {
          context.fail(delete.cause());
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordDeleted(eq(save.result()), any());

        recordService.getRecordById(sourceRecord.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isEmpty());
        });
        async.complete();
      });
    });
  }

  @Test
  public void shouldUnDeleteMarcRecord(TestContext context) {
    Async async = context.async();
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

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(sourceRecord, okapiHeaders).onComplete(saveResult -> {
      if (saveResult.failed()) {
        context.fail(saveResult.cause());
      }
      recordService.unDeleteRecordById(sourceRecord.getId(), IdType.RECORD, okapiHeaders).onComplete(undeleteResult -> {
        if (undeleteResult.failed()) {
          context.fail(undeleteResult.cause());
        }
        recordService.getRecordById(sourceRecord.getId(), TENANT_ID).onComplete(getResult -> {
          if (getResult.failed()) {
            context.fail(getResult.cause());
          }
          context.assertTrue(getResult.result().isPresent());
          context.assertFalse(getResult.result().get().getDeleted());
          verify(recordDomainEventPublisher, times(1))
            .publishRecordUpdated(eq(saveResult.result()), eq(getResult.result().get()), any());
        });
        async.complete();
      });
    });
  }

  @Test
  public void shouldSoftDeleteMarcRecord(TestContext context) {
    Async async = context.async();
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

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(sourceRecord, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }

      recordService.deleteRecordById(sourceRecord.getId(), IdType.RECORD, okapiHeaders).onComplete(delete -> {
        if (delete.failed()) {
          context.fail(delete.cause());
        }
        recordService.getRecordById(sourceRecord.getId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }

          context.assertTrue(get.result().isPresent());
          context.assertTrue(get.result().get().getDeleted());
          verify(recordDomainEventPublisher, times(1))
            .publishRecordUpdated(eq(save.result()), eq(get.result().get()), any());
        });
        async.complete();
      });
    });
  }

  private void getTotalRecordsAndRecordsDependsOnLimit(TestContext context, int limit) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ID.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.MARC_BIB, orderFields, 0, limit, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC_BIB))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        context.assertEquals(limit, get.result().getRecords().size());
        async.complete();
      });
    });
  }

  private void getRecordsBySnapshotId(TestContext context, String snapshotId, RecordType parsedRecordType,
                                      Record.RecordType recordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, parsedRecordType, orderFields, 0, 1, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .sorted(comparing(Record::getOrder))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.getFirst(), get.result().getRecords().getFirst());
        async.complete();
      });
    });
  }

  private void getMarcRecordsBetweenDates(TestContext context, OffsetDateTime earliestDate, OffsetDateTime latestDate,
                                          RecordType parsedRecordType, Record.RecordType recordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = RECORDS_LB.CREATED_DATE.between(earliestDate, latestDate);
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, parsedRecordType, orderFields, 0, 15, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected, get.result().getRecords());
        async.complete();
      });
    });
  }

  private void streamRecordsBySnapshotId(TestContext context, String snapshotId, RecordType parsedRecordType,
                                         Record.RecordType recordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
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

          context.assertEquals(expected.size(), actual.size());
          compareRecords(context, expected.getFirst(), actual.getFirst());

          async.complete();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  private void getMarcRecordById(TestContext context, Record expected) {
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  private void saveMarcRecord(TestContext context, Record expected, Record.RecordType marcBib) {
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertNotNull(save.result().getRawRecord());
      context.assertNotNull(save.result().getParsedRecord());
      compareRecords(context, expected, save.result());
      recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        verify(recordDomainEventPublisher, times(1)).publishRecordCreated(eq(save.result()), any());
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertEquals(marcBib, get.result().get().getRecordType());
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  private void saveMarcRecordWithGenerationGreaterThanZero(TestContext context, Record expected, Record.RecordType marcBib) {
    Async async = context.async();
    expected.setGeneration(1);
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordService.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertNotNull(save.result().getRawRecord());
      context.assertNotNull(save.result().getParsedRecord());
      compareRecords(context, expected, save.result());
      recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertEquals(marcBib, get.result().get().getRecordType());
        context.assertTrue(get.result().get().getGeneration() > 0);
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  private void saveMarcRecords(TestContext context, Record.RecordType marcBib) {
    Async async = context.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(marcBib))
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordService.saveRecords(recordCollection, okapiHeaders).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }

      ArgumentCaptor<Record> captureOldRecord = ArgumentCaptor.forClass(Record.class);
      verify(recordDomainEventPublisher, times(batch.result().getTotalRecords())).publishRecordCreated(captureOldRecord.capture(), any());
      compareRecords(context, captureOldRecord.getAllValues(), expected);
      context.assertEquals(0, batch.result().getErrorMessages().size());
      context.assertEquals(expected.size(), batch.result().getTotalRecords());
      compareRecords(context, expected, batch.result().getRecords());
      RecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition())
        .onComplete(count -> {
          if (count.failed()) {
            context.fail(count.cause());
          }
          context.assertEquals(expected.size(), count.result());
          async.complete();
        });
    });
  }

  private void saveMarcRecordsWithExpectedErrors(TestContext context, Record.RecordType recordType) {
    Async async = context.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(recordType))
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .map(record -> record.withErrorRecord(TestMocks.getErrorRecord(0)))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordService.saveRecords(recordCollection, okapiHeaders).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      context.assertEquals(0, batch.result().getErrorMessages().size());
      context.assertEquals(expected.size(), batch.result().getTotalRecords());
      compareRecords(context, expected, batch.result().getRecords());
      checkRecordErrorRecords(context, batch.result().getRecords(), TestMocks.getErrorRecord(0).getContent().toString(),
        TestMocks.getErrorRecord(0).getDescription());
      RecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition())
        .onComplete(count -> {
          if (count.failed()) {
            context.fail(count.cause());
          }
          context.assertEquals(expected.size(), count.result());
          async.complete();
        });
    });
  }

  private void checkRecordErrorRecords(TestContext context, List<Record> actual, String expectedErrorContent,
                                       String expectedErrorDescription) {
    for (Record record : actual) {
      context.assertEquals(expectedErrorContent, record.getErrorRecord().getContent());
      context.assertEquals(expectedErrorDescription, record.getErrorRecord().getDescription());
    }
  }

  private void getMarcSourceRecords(TestContext context, RecordType parsedRecordType, Record.RecordType recordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }

      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  private void streamMarcSourceRecords(TestContext context, RecordType parsedRecordType, Record.RecordType recordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
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
          context.assertEquals(expected.size(), actual.size());
          compareSourceRecords(context, expected, actual);

          async.complete();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  private void getMarcSourceRecordsByListOfIds(TestContext context, Record.RecordType recordType,
                                               RecordType parsedRecordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      List<String> ids = records.stream()
        .filter(r -> r.getRecordType().equals(recordType))
        .map(Record::getMatchedId)
        .collect(Collectors.toList());

      recordService.getSourceRecords(ids, IdType.RECORD, parsedRecordType, false, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  private void getMarcSourceRecordsBetweenDates(TestContext context, Record.RecordType recordType,
                                                RecordType parsedRecordType, OffsetDateTime earliestDate,
                                                OffsetDateTime latestDate) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }

      Condition condition = RECORDS_LB.CREATED_DATE.between(earliestDate, latestDate);
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  private void getMarcSourceRecordsByListOfIdsThatAreDeleted(TestContext context, Record.RecordType recordType,
                                                             RecordType parsedRecordType) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords().stream()
      .map(record -> {
        Record deletedRecord = new Record()
          .withId(record.getId())
          .withSnapshotId(record.getSnapshotId())
          .withMatchedId(record.getMatchedId())
          .withRecordType(record.getRecordType())
          .withState(State.DELETED)
          .withGeneration(record.getGeneration())
          .withOrder(record.getOrder())
          .withLeaderRecordStatus(record.getLeaderRecordStatus())
          .withRawRecord(record.getRawRecord())
          .withParsedRecord(record.getParsedRecord())
          .withAdditionalInfo(record.getAdditionalInfo())
          .withExternalIdsHolder(record.getExternalIdsHolder());
        if (Objects.nonNull(record.getMetadata())) {
          deletedRecord.withMetadata(record.getMetadata());
        }
        if (Objects.nonNull(record.getErrorRecord())) {
          deletedRecord.withErrorRecord(record.getErrorRecord());
        }
        return deletedRecord;
      })
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      List<String> ids = records.stream()
        .filter(r -> r.getRecordType().equals(recordType))
        .map(Record::getMatchedId)
        .collect(Collectors.toList());
      recordService.getSourceRecords(ids, IdType.RECORD, parsedRecordType, true, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .map(RecordDaoUtil::toSourceRecord)
          .toList();
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  private void getMarcSourceRecordById(TestContext context, Record expected) {
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService
        .getSourceRecordById(expected.getMatchedId(), IdType.RECORD, RecordState.ACTUAL, TENANT_ID)
        .onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertTrue(get.result().isPresent());
          context.assertNotNull(get.result().get().getParsedRecord());
          compareSourceRecords(context, RecordDaoUtil.toSourceRecord(expected), get.result().get());
          async.complete();
        });
    });
  }

  private void notGetMarcSourceRecordById(TestContext context, Record expected) {
    Async async = context.async();
    recordService
      .getSourceRecordById(expected.getMatchedId(), IdType.RECORD, RecordState.ACTUAL, TENANT_ID)
      .onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertFalse(get.result().isPresent());
        async.complete();
      });
  }

  private void updateParsedMarcRecords(TestContext context, Record.RecordType recordType) {
    Async async = context.async();
    List<Record> original = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(recordType))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      List<Record> updated = original.stream()
        .map(aRecord -> clone(aRecord, Record.class))
        .map(aRecord -> aRecord
          .withExternalIdsHolder(aRecord.getExternalIdsHolder().withInstanceId(UUID.randomUUID().toString())))
        .collect(Collectors.toList());
      recordCollection
        .withRecords(updated)
        .withTotalRecords(updated.size());
      List<ParsedRecord> expected = updated.stream()
        .map(Record::getParsedRecord)
        .collect(Collectors.toList());
      var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
      recordService.updateParsedRecords(recordCollection, okapiHeaders).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }

        ArgumentCaptor<Record> captureOldRecords = ArgumentCaptor.forClass(Record.class);
        ArgumentCaptor<Record> captureNewRecords = ArgumentCaptor.forClass(Record.class);

        verify(recordDomainEventPublisher, times(update.result().getTotalRecords()))
          .publishRecordUpdated(captureOldRecords.capture(), captureNewRecords.capture(), any());

        compareRecords(context, captureOldRecords.getAllValues(), original);
        compareRecords(context, captureNewRecords.getAllValues(), updated);

        context.assertEquals(0, update.result().getErrorMessages().size());
        context.assertEquals(expected.size(), update.result().getTotalRecords());
        compareParsedRecords(context, expected, update.result().getParsedRecords());
        GenericCompositeFuture.all(updated.stream().map(record -> recordDao
          .getRecordByMatchedId(record.getMatchedId(), TENANT_ID)
          .onComplete(get -> {
            if (get.failed()) {
              context.fail(get.cause());
            }
            context.assertTrue(get.result().isPresent());
          })).collect(Collectors.toList())).onComplete(res -> {
          if (res.failed()) {
            context.fail(res.cause());
          }
          async.complete();
        });
      });
    });
  }

  private void updateParsedMarcRecordsAndGetOnlyActualRecord(TestContext context, Record expected) {
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      context.assertTrue(save.succeeded());
      expected.setLeaderRecordStatus("a");
      recordService.updateRecord(expected, okapiHeaders)
        .compose(v -> recordService.getFormattedRecord(expected.getMatchedId(), IdType.RECORD, TENANT_ID))
        .onComplete(get -> {
          context.assertTrue(get.succeeded());
          context.assertNotNull(get.result().getParsedRecord());
          context.assertEquals(expected.getParsedRecord().getFormattedContent(),
            get.result().getParsedRecord().getFormattedContent());
          context.assertEquals(get.result().getState().toString(), "ACTUAL");
          async.complete();
        });
    });
  }

  private void getFormattedMarcRecord(TestContext context, Record expected) {
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService
        .getFormattedRecord(expected.getMatchedId(), IdType.RECORD, TENANT_ID)
        .onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          context.assertNotNull(get.result().getParsedRecord());
          context.assertEquals(expected.getParsedRecord().getFormattedContent(),
            get.result().getParsedRecord().getFormattedContent());
          async.complete();
        });
    });
  }
  private void updateSuppressFromDiscoveryForMarcRecord(TestContext context, Record expected) {
    Async async = context.async();
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);

    recordDao.saveRecord(expected, okapiHeaders).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.updateSuppressFromDiscoveryForRecord(expected.getMatchedId(), IdType.RECORD, true, TENANT_ID)
        .onComplete(update -> {
          if (update.failed()) {
            context.fail(update.cause());
          }
          context.assertTrue(update.result());
          recordDao.getRecordById(expected.getMatchedId(), TENANT_ID)
            .onComplete(get -> {
              if (get.failed()) {
                context.fail(get.cause());
              }
              verify(recordDomainEventPublisher, times(0)).publishRecordUpdated(any(), any(), any());
              context.assertTrue(get.result().isPresent());
              context.assertNotNull(get.result().get().getRawRecord());
              context.assertNotNull(get.result().get().getParsedRecord());
              expected.setAdditionalInfo(expected.getAdditionalInfo().withSuppressDiscovery(true));
              compareRecords(context, expected, get.result().get());
              async.complete();
            });
        });
    });
  }

  private void deleteMarcRecordsBySnapshotId(TestContext context, String snapshotId, RecordType parsedRecordType,
                                             Record.RecordType recordType) {
    Async async = context.async();
    List<Record> original = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    saveRecords(recordCollection.getRecords()).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordDao.getRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(getBefore -> {
        if (getBefore.failed()) {
          context.fail(getBefore.cause());
        }
        int expected = (int) original.stream()
          .filter(r -> r.getRecordType().equals(recordType))
          .filter(record -> record.getSnapshotId().equals(snapshotId))
          .count();
        context.assertTrue(expected > 0);
        context.assertEquals(expected, getBefore.result().getTotalRecords());
        recordService.deleteRecordsBySnapshotId(snapshotId, TENANT_ID).onComplete(delete -> {
          if (delete.failed()) {
            context.fail(delete.cause());
          }
          context.assertTrue(delete.result());
          recordDao.getRecords(condition, parsedRecordType, orderFields, 0, 10, TENANT_ID).onComplete(getAfter -> {
            if (getAfter.failed()) {
              context.fail(getAfter.cause());
            }
            context.assertEquals(0, getAfter.result().getTotalRecords());
            SnapshotDaoUtil.findById(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshotId)
              .onComplete(getSnapshot -> {
                if (getSnapshot.failed()) {
                  context.fail(getSnapshot.cause());
                }
                context.assertFalse(getSnapshot.result().isPresent());
                async.complete();
              });
          });
        });
      });
    });
  }

  private CompositeFuture saveRecords(List<Record> records) {
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    return GenericCompositeFuture.all(records.stream()
      .map(record -> recordService.saveRecord(record, okapiHeaders))
      .collect(Collectors.toList())
    );
  }

  private void compareRecords(TestContext context, List<Record> expected, List<Record> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (Record record : expected) {
      var actualRecord = actual.stream()
        .filter(r -> Objects.equals(r.getId(), record.getId()))
        .findFirst();
      actualRecord.ifPresent(value -> compareRecords(context, record, value));
    }
  }

  private void compareRecords(TestContext context, Record expected, Record actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getMatchedId(), actual.getMatchedId());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    context.assertEquals(expected.getState(), actual.getState());
    context.assertEquals(expected.getLeaderRecordStatus(), actual.getLeaderRecordStatus());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    context.assertEquals(expected.getGeneration(), actual.getGeneration());
    if (Objects.nonNull(expected.getRawRecord())) {
      compareRawRecords(context, expected.getRawRecord(), actual.getRawRecord());
    } else {
      context.assertNull(actual.getRawRecord());
    }
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(context, expected.getParsedRecord(), actual.getParsedRecord());
    } else {
      context.assertNull(actual.getParsedRecord());
    }
    if (Objects.nonNull(expected.getErrorRecord())) {
      compareErrorRecords(context, expected.getErrorRecord(), actual.getErrorRecord());
    } else {
      context.assertNull(actual.getErrorRecord());
    }
    if (Objects.nonNull(expected.getAdditionalInfo())) {
      compareAdditionalInfo(context, expected.getAdditionalInfo(), actual.getAdditionalInfo());
    } else {
      context.assertNull(actual.getAdditionalInfo());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      compareExternalIdsHolder(context, expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    } else {
      context.assertNull(actual.getExternalIdsHolder());
    }
    if (Objects.nonNull(expected.getMetadata())) {
      compareMetadata(context, expected.getMetadata(), actual.getMetadata());
    } else {
      context.assertNull(actual.getMetadata());
    }
  }

  private void compareRecords(TestContext context, Record expected, StrippedParsedRecord actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getRecordType().toString(), actual.getRecordType().toString());
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(context, expected.getParsedRecord(), actual.getParsedRecord());
    } else {
      context.assertNull(actual.getParsedRecord());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      compareExternalIdsHolder(context, expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    } else {
      context.assertNull(actual.getExternalIdsHolder());
    }
  }

  private void compareSourceRecords(TestContext context, List<SourceRecord> expected, List<SourceRecord> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (SourceRecord sourceRecord : expected) {
      var sourceRecordActual = actual.stream()
        .filter(sr -> Objects.equals(sr.getRecordId(), sourceRecord.getRecordId()))
        .findFirst();
      sourceRecordActual.ifPresent(record -> compareSourceRecords(context, sourceRecord, record));
    }
  }

  private void compareSourceRecords(TestContext context, SourceRecord expected, SourceRecord actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getRecordId(), actual.getRecordId());
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(context, expected.getParsedRecord(), actual.getParsedRecord());
    }
    if (Objects.nonNull(expected.getAdditionalInfo())) {
      compareAdditionalInfo(context, expected.getAdditionalInfo(), actual.getAdditionalInfo());
    } else {
      context.assertNull(actual.getAdditionalInfo());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      compareExternalIdsHolder(context, expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    } else {
      context.assertNull(actual.getExternalIdsHolder());
    }
    if (Objects.nonNull(expected.getMetadata())) {
      compareMetadata(context, expected.getMetadata(), actual.getMetadata());
    } else {
      context.assertNull(actual.getMetadata());
    }
  }

  private void compareParsedRecords(TestContext context, List<ParsedRecord> expected, List<ParsedRecord> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (ParsedRecord parsedRecord : expected) {
      var actualParsedRecord = actual.stream().filter(a -> Objects.equals(a.getId(), parsedRecord.getId())).findFirst();
      actualParsedRecord.ifPresent(record -> compareParsedRecords(context, parsedRecord, record));
    }
  }

  private void compareRawRecords(TestContext context, RawRecord expected, RawRecord actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
  }

  private void compareParsedRecords(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(ParsedRecordDaoUtil.normalizeContent(expected), ParsedRecordDaoUtil.normalizeContent(actual));
  }

  private void compareErrorRecords(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
    context.assertEquals(expected.getDescription(), actual.getDescription());
  }

  private void compareAdditionalInfo(TestContext context, AdditionalInfo expected, AdditionalInfo actual) {
    context.assertEquals(expected.getSuppressDiscovery(), actual.getSuppressDiscovery());
  }

  private void compareExternalIdsHolder(TestContext context, ExternalIdsHolder expected, ExternalIdsHolder actual) {
    context.assertEquals(expected.getInstanceId(), actual.getInstanceId());
  }

  private static <T> T clone(T obj, Class<T> type) {
    try {
      final ObjectMapper jsonMapper = ObjectMapperTool.getMapper();
      return jsonMapper.readValue(jsonMapper.writeValueAsString(obj), type);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException(ex);
    }
  }
}
