package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.IdType;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
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
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.StrippedParsedRecord;
import org.folio.rest.jooq.enums.RecordState;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.services.RecordServiceImpl.INDICATOR;
import static org.folio.services.RecordServiceImpl.SUBFIELD_S;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.AdditionalFieldsUtil.getFieldFromMarcRecord;
import static org.junit.Assert.assertThrows;

@RunWith(VertxUnitRunner.class)
public class RecordServiceTest extends AbstractLBServiceTest {

  private static final String MARC_BIB_RECORD_SNAPSHOT_ID = "d787a937-cc4b-49b3-85ef-35bcd643c689";
  private static final String MARC_AUTHORITY_RECORD_SNAPSHOT_ID = "ee561342-3098-47a8-ab6e-0f3eba120b04";
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  private RecordDao recordDao;

  private RecordService recordService;

  private static RawRecord rawRecord;
  private static ParsedRecord marcRecord;

  @Before
  public void setUp(TestContext context) throws IOException {
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    marcRecord = new ParsedRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());

    recordDao = new RecordDaoImpl(postgresClientFactory);
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
          .collect(Collectors.toList());
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
        compareRecords(context, expected.get(0), get.result().getRecords().get(0));
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
        compareRecords(context, expected.get(0), get.result().getRecords().get(0));
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
        .collect(Collectors.toList());

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

    recordService.saveRecord(record, TENANT_ID, null).onComplete(save -> {
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
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()))
      .withMetadata(original.getMetadata());
    Async async = context.async();

    recordService.saveRecord(record, TENANT_ID, null).onComplete(save -> {
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
  public void shouldSaveMarcBibRecordWithMatchedIdFromExistingSourceRecord(TestContext context) throws IOException {
    Async async = context.async();
    Record original = TestMocks.getMarcBibRecord();
    String recordId1 = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder().withInstanceId(instanceId);
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
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
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

    recordService.saveRecord(record1, TENANT_ID, null).onComplete(save -> {
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

        recordService.saveRecord(record2, TENANT_ID, null).onComplete(save2 -> {
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
    recordService.saveRecord(invalid, TENANT_ID, null).onComplete(save -> {
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
    recordDao.saveRecord(original, TENANT_ID).onComplete(save -> {
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
      recordService.updateRecord(expected, TENANT_ID).onComplete(update -> {
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

    recordDao.saveRecord(original, TENANT_ID)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, TENANT_ID))
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, TENANT_ID))
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

    recordDao.saveRecord(original, TENANT_ID)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, TENANT_ID))
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
    recordDao.saveRecord(original, TENANT_ID).onComplete(save -> {
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
      recordService.updateRecord(expected, TENANT_ID).onComplete(update -> {
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
    recordDao.getRecordById(record.getMatchedId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
      recordService.updateRecord(record, TENANT_ID).onComplete(update -> {
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

    recordDao.saveRecord(expected, TENANT_ID)
      .compose(ar -> recordService.updateSourceRecord(parsedRecordDto, snapshotId, TENANT_ID))
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
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
      recordService.updateSourceRecord(parsedRecordDto, snapshotId, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
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
    List<Future<RecordsBatchResponse>> futures = List.of(recordService.saveRecords(recordCollection, TENANT_ID),
      recordService.saveRecords(recordCollection, TENANT_ID));

    GenericCompositeFuture.all(futures).onComplete(ar -> {
      context.assertTrue(ar.failed());
      assertThrows(DuplicateEventException.class, () -> {throw ar.cause();});
      async.complete();
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
          .collect(Collectors.toList());
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
          .collect(Collectors.toList());
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.get(0), get.result().getRecords().get(0));
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
          .sorted(comparing(Record::getOrder))
          .collect(Collectors.toList());
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
        .collect(Collectors.toList());

      List<Record> actual = new ArrayList<>();
      flowable.doFinally(() -> {

          context.assertEquals(expected.size(), actual.size());
          compareRecords(context, expected.get(0), actual.get(0));

          async.complete();

        }).collect(() -> actual, List::add)
        .subscribe();
    });
  }

  private void getMarcRecordById(TestContext context, Record expected) {
    Async async = context.async();
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
    recordService.saveRecord(expected, TENANT_ID, null).onComplete(save -> {
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
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  private void saveMarcRecordWithGenerationGreaterThanZero(TestContext context, Record expected, Record.RecordType marcBib) {
    Async async = context.async();
    expected.setGeneration(1);
    recordService.saveRecord(expected, TENANT_ID, null).onComplete(save -> {
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
    recordService.saveRecords(recordCollection, TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      context.assertEquals(0, batch.result().getErrorMessages().size());
      context.assertEquals(expected.size(), batch.result().getTotalRecords());
      expected.sort(comparing(Record::getId));
      batch.result().getRecords().sort(comparing(Record::getId));
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
    recordService.saveRecords(recordCollection, TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      context.assertEquals(0, batch.result().getErrorMessages().size());
      context.assertEquals(expected.size(), batch.result().getTotalRecords());
      expected.sort(comparing(Record::getId));
      batch.result().getRecords().sort(comparing(Record::getId));
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
          .sorted(comparing(SourceRecord::getRecordId))
          .collect(Collectors.toList());
        get.result().getSourceRecords().sort(comparing(SourceRecord::getRecordId));
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
        .sorted(comparing(SourceRecord::getRecordId))
        .sorted(comparing(SourceRecord::getOrder))
        .collect(Collectors.toList());

      List<SourceRecord> actual = new ArrayList<>();
      flowable.doFinally(() -> {

          actual.sort(comparing(SourceRecord::getRecordId));
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
          .sorted(comparing(SourceRecord::getRecordId))
          .collect(Collectors.toList());
        sortByRecordId(get);
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  private void sortByRecordId(AsyncResult<SourceRecordCollection> get) {
    get.result().getSourceRecords().sort(comparing(SourceRecord::getRecordId));
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
          .sorted(comparing(SourceRecord::getRecordId))
          .collect(Collectors.toList());
        get.result().getSourceRecords().sort(comparing(SourceRecord::getRecordId));
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
          .sorted(comparing(SourceRecord::getRecordId))
          .collect(Collectors.toList());
        get.result().getSourceRecords().sort(comparing(SourceRecord::getRecordId));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  private void getMarcSourceRecordById(TestContext context, Record expected) {
    Async async = context.async();
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
          context.assertNotNull(get.result().get().getRawRecord());
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
        .map(record -> record
          .withExternalIdsHolder(record.getExternalIdsHolder().withInstanceId(UUID.randomUUID().toString())))
        .collect(Collectors.toList());
      recordCollection
        .withRecords(updated)
        .withTotalRecords(updated.size());
      List<ParsedRecord> expected = updated.stream()
        .map(Record::getParsedRecord)
        .collect(Collectors.toList());
      recordService.updateParsedRecords(recordCollection, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertEquals(0, update.result().getErrorMessages().size());
        context.assertEquals(expected.size(), update.result().getTotalRecords());
        expected.sort(comparing(ParsedRecord::getId));
        update.result().getParsedRecords().sort(comparing(ParsedRecord::getId));
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
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      context.assertTrue(save.succeeded());
      expected.setLeaderRecordStatus("a");
      recordService.updateRecord(expected, TENANT_ID)
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
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
    return GenericCompositeFuture.all(records.stream()
      .map(record -> recordService.saveRecord(record, AbstractLBServiceTest.TENANT_ID))
      .collect(Collectors.toList())
    );
  }

  private void compareRecords(TestContext context, List<Record> expected, List<Record> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (Record record : expected) {
      compareRecords(context, record, record);
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
      compareSourceRecords(context, sourceRecord, sourceRecord);
    }
  }

  private void compareSourceRecords(TestContext context, SourceRecord expected, SourceRecord actual) {
    context.assertNotNull(actual);
    context.assertEquals(expected.getRecordId(), actual.getRecordId());
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    if (Objects.nonNull(expected.getRawRecord())) {
      compareRawRecords(context, expected.getRawRecord(), actual.getRawRecord());
    } else {
      context.assertNull(actual.getRawRecord());
    }
    if (Objects.nonNull(expected.getParsedRecord())) {
      compareParsedRecords(context, expected.getParsedRecord(), actual.getParsedRecord());
    } else {
      context.assertNull(actual.getRawRecord());
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
      compareParsedRecords(context, parsedRecord, parsedRecord);
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
}
