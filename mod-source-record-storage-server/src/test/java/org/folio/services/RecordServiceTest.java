package org.folio.services;

import static org.folio.rest.jooq.Tables.RECORDS_LB;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.TestMocks;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jooq.enums.RecordState;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.reactivex.Flowable;
import io.vertx.core.CompositeFuture;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RecordServiceTest extends AbstractLBServiceTest {

  private RecordDao recordDao;

  private RecordService recordService;

  @Before
  public void setUp(TestContext context) {
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
  public void shouldGetMarcRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.MARC, orderFields, 1, 2, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .collect(Collectors.toList());
        Collections.sort(expected, (r1, r2) -> r1.getOrder().compareTo(r2.getOrder()));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.get(1), get.result().getRecords().get(0));
        compareRecords(context, expected.get(2), get.result().getRecords().get(1));
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetEdifactRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = "dcd898af-03bb-4b12-b8a6-f6a02e86459b";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      recordService.getRecords(condition, RecordType.EDIFACT, orderFields, 0, 1, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.EDIFACT))
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .collect(Collectors.toList());
        Collections.sort(expected, (r1, r2) -> r1.getOrder().compareTo(r2.getOrder()));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, expected.get(0), get.result().getRecords().get(0));
        async.complete();
      });
    });
  }

  @Test
  public void shouldStreamMarcRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      Flowable<Record> flowable = recordService.streamRecords(condition, RecordType.MARC, orderFields, 0, 10, TENANT_ID);

      List<Record> expected = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
        .filter(r -> r.getSnapshotId().equals(snapshotId))
        .collect(Collectors.toList());

      Collections.sort(expected, (r1, r2) -> r1.getOrder().compareTo(r2.getOrder()));

      List<Record> actual = new ArrayList<>();
      flowable.doFinally(() -> {

        context.assertEquals(expected.size(), actual.size());
        compareRecords(context, expected.get(0), actual.get(0));
        compareRecords(context, expected.get(1), actual.get(1));
        compareRecords(context, expected.get(2), actual.get(2));

        async.complete();

      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
    });
  }

  @Test
  public void shouldStreamEdifactRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = "dcd898af-03bb-4b12-b8a6-f6a02e86459b";
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(RECORDS_LB.ORDER.sort(SortOrder.ASC));
      Flowable<Record> flowable = recordService.streamRecords(condition, RecordType.EDIFACT, orderFields, 0, 10, TENANT_ID);

      List<Record> expected = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.EDIFACT))
        .filter(r -> r.getSnapshotId().equals(snapshotId))
        .collect(Collectors.toList());

      Collections.sort(expected, (r1, r2) -> r1.getOrder().compareTo(r2.getOrder()));

      List<Record> actual = new ArrayList<>();
      flowable.doFinally(() -> {

        context.assertEquals(expected.size(), actual.size());
        compareRecords(context, expected.get(0), actual.get(0));

        async.complete();

      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
    });
  }

  // TODO: test get records between two dates

  @Test
  public void shouldGetRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
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

  // TODO: test get by matched id not equal to id

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
  public void shouldSaveMarcRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
        context.assertEquals(Record.RecordType.MARC, get.result().get().getRecordType());
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveEdifactRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getEdifactRecord();
    recordService.saveRecord(expected, TENANT_ID).onComplete(save -> {
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
        context.assertEquals(Record.RecordType.EDIFACT, get.result().get().getRecordType());
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  // TODO: test save record with calculate generation greater than 0

  @Test
  public void shouldFailToSaveRecord(TestContext context) {
    Async async = context.async();
    Record valid = TestMocks.getRecord(0);
    Record invalid = new Record()
      .withId(valid.getId())
      .withSnapshotId(valid.getSnapshotId())
      .withMatchedId(valid.getMatchedId())
      .withState(valid.getState())
      .withGeneration(valid.getGeneration())
      .withOrder(valid.getOrder())
      .withRawRecord(valid.getRawRecord())
      .withParsedRecord(valid.getParsedRecord())
      .withAdditionalInfo(valid.getAdditionalInfo())
      .withExternalIdsHolder(valid.getExternalIdsHolder())
      .withMetadata(valid.getMetadata());
    recordService.saveRecord(invalid, TENANT_ID).onComplete(save -> {
      context.assertTrue(save.failed());
      String expected = "null value in column \"record_type\" violates not-null constraint";
      context.assertEquals(expected, save.cause().getMessage());
      async.complete();
    });
  }

  @Test
  public void shouldSaveMarcRecords(TestContext context) {
    Async async = context.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC))
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
      Collections.sort(expected, (r1, r2) -> r1.getId().compareTo(r2.getId()));
      Collections.sort(batch.result().getRecords(), (r1, r2) -> r1.getId().compareTo(r2.getId()));
      compareRecords(context, expected, batch.result().getRecords());
      RecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition()).onComplete(count -> {
        if (count.failed()) {
          context.fail(count.cause());
        }
        context.assertEquals(expected.size(), count.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveEdifactRecords(TestContext context) {
    Async async = context.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.EDIFACT))
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
      Collections.sort(expected, (r1, r2) -> r1.getId().compareTo(r2.getId()));
      Collections.sort(batch.result().getRecords(), (r1, r2) -> r1.getId().compareTo(r2.getId()));
      compareRecords(context, expected, batch.result().getRecords());
      RecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition()).onComplete(count -> {
        if (count.failed()) {
          context.fail(count.cause());
        }
        context.assertEquals(expected.size(), count.result());
        async.complete();
      });
    });
  }

  // TODO: test save records with expected errors

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

  // TODO: test update record with calculate generation greater than 0

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
  public void shouldGetMarcSourceRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, RecordType.MARC, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
          .map(RecordDaoUtil::toSourceRecord)
          .collect(Collectors.toList());
        Collections.sort(expected, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        Collections.sort(get.result().getSourceRecords(), (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetEdifactSourceRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, RecordType.EDIFACT, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.EDIFACT))
          .map(RecordDaoUtil::toSourceRecord)
          .collect(Collectors.toList());
        Collections.sort(expected, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        Collections.sort(get.result().getSourceRecords(), (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  @Test
  public void shouldStreamMarcSourceRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();

      Flowable<SourceRecord> flowable = recordService.streamSourceRecords(condition, RecordType.MARC, orderFields, 0, 10, TENANT_ID);

      List<SourceRecord> expected = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
        .map(RecordDaoUtil::toSourceRecord)
        .collect(Collectors.toList());
      Collections.sort(expected, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));

      Collections.sort(expected, (r1, r2) -> r1.getOrder().compareTo(r2.getOrder()));

      List<SourceRecord> actual = new ArrayList<>();
      flowable.doFinally(() -> {

        Collections.sort(actual, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        context.assertEquals(expected.size(), actual.size());
        compareSourceRecords(context, expected, actual);

        async.complete();

      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
    });
  }

  @Test
  public void shouldStreamEdifactSourceRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();

      Flowable<SourceRecord> flowable = recordService.streamSourceRecords(condition, RecordType.EDIFACT, orderFields, 0, 10, TENANT_ID);

      List<SourceRecord> expected = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.EDIFACT))
        .map(RecordDaoUtil::toSourceRecord)
        .collect(Collectors.toList());
      Collections.sort(expected, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));

      Collections.sort(expected, (r1, r2) -> r1.getOrder().compareTo(r2.getOrder()));

      List<SourceRecord> actual = new ArrayList<>();
      flowable.doFinally(() -> {

        Collections.sort(actual, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        context.assertEquals(expected.size(), actual.size());
        compareSourceRecords(context, expected, actual);

        async.complete();

      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
    });
  }

  @Test
  public void shouldGetSourceRecordsByListOfIds(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      List<String> ids = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
        .map(record -> record.getExternalIdsHolder().getInstanceId())
        .collect(Collectors.toList());
      recordService.getSourceRecords(ids, ExternalIdType.INSTANCE, RecordType.MARC, false, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
          .map(RecordDaoUtil::toSourceRecord)
          .collect(Collectors.toList());
        Collections.sort(expected, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        Collections.sort(get.result().getSourceRecords(), (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetSourceRecordsByListOfIdsThatAreDeleted(TestContext context) {
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
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      List<String> ids = records.stream()
        .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
        .map(record -> record.getExternalIdsHolder().getInstanceId())
        .collect(Collectors.toList());
      recordService.getSourceRecords(ids, ExternalIdType.INSTANCE, RecordType.MARC, true, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
          .map(RecordDaoUtil::toSourceRecord)
          .collect(Collectors.toList());
        Collections.sort(expected, (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        Collections.sort(get.result().getSourceRecords(), (r1, r2) -> r1.getRecordId().compareTo(r2.getRecordId()));
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareSourceRecords(context, expected, get.result().getSourceRecords());
        async.complete();
      });
    });
  }

  // TODO: test get source records between two dates

  @Test
  public void shouldGetSourceRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getSourceRecordById(expected.getExternalIdsHolder().getInstanceId(), ExternalIdType.INSTANCE, TENANT_ID).onComplete(get -> {
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

  // TODO: test get by matched id not equal to id

  @Test
  public void shouldNotGetSourceRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.getSourceRecordById(expected.getExternalIdsHolder().getInstanceId(), ExternalIdType.INSTANCE, TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateParsedRecords(TestContext context) {
    Async async = context.async();
    List<Record> original = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      List<Record> updated = original.stream()
        .map(record -> record.withExternalIdsHolder(record.getExternalIdsHolder().withInstanceId(UUID.randomUUID().toString())))
        .collect(Collectors.toList());
      recordCollection
        .withRecords(updated)
        .withTotalRecords(updated.size());
      List<ParsedRecord> expected = updated.stream()
        .map(record -> record.getParsedRecord()).collect(Collectors.toList());
      recordService.updateParsedRecords(recordCollection, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertEquals(0, update.result().getErrorMessages().size());
        context.assertEquals(expected.size(), update.result().getTotalRecords());
        Collections.sort(expected, (r1, r2) -> r1.getId().compareTo(r2.getId()));
        Collections.sort(update.result().getParsedRecords(), (r1, r2) -> r1.getId().compareTo(r2.getId()));
        compareParsedRecords(context, expected, update.result().getParsedRecords());
        CompositeFuture.all(updated.stream().map(record -> recordDao.getRecordByExternalId(record.getExternalIdsHolder().getInstanceId(), ExternalIdType.INSTANCE, TENANT_ID).onComplete(get -> {
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

  @Test
  public void shouldGetFormattedMarcRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getFormattedRecord(expected.getExternalIdsHolder().getInstanceId(), ExternalIdType.INSTANCE, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertNotNull(get.result().getParsedRecord());
        context.assertEquals(expected.getParsedRecord().getFormattedContent(), get.result().getParsedRecord().getFormattedContent());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetFormattedEdifactRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getEdifactRecord();
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getFormattedRecord(expected.getId(), ExternalIdType.RECORD, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertNotNull(get.result().getParsedRecord());
        context.assertEquals(expected.getParsedRecord().getFormattedContent(), get.result().getParsedRecord().getFormattedContent());
        async.complete();
      });
    });
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      String instanceId = expected.getExternalIdsHolder().getInstanceId();
      Boolean suppress = true;
      recordService.updateSuppressFromDiscoveryForRecord(instanceId, ExternalIdType.INSTANCE, suppress, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertTrue(update.result());
        recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
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

  @Test
  public void shouldDeleteRecordsBySnapshotId(TestContext context) {
    Async async = context.async();
    List<Record> original = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    saveRecords(recordCollection.getRecords(), TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = TestMocks.getSnapshot(3).getJobExecutionId();
      Condition condition = RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordDao.getRecords(condition, RecordType.MARC, orderFields, 0, 10, TENANT_ID).onComplete(getBefore -> {
        if (getBefore.failed()) {
          context.fail(getBefore.cause());
        }
        Integer expected = (int) original.stream()
          .filter(r -> r.getRecordType().equals(Record.RecordType.MARC))
          .filter(record -> record.getSnapshotId().equals(snapshotId))
          .count();
        context.assertTrue(expected > 0);
        context.assertEquals(expected, getBefore.result().getTotalRecords());
        recordService.deleteRecordsBySnapshotId(snapshotId, TENANT_ID).onComplete(delete -> {
          if (delete.failed()) {
            context.fail(delete.cause());
          }
          context.assertTrue(delete.result());
          recordDao.getRecords(condition, RecordType.MARC, orderFields, 0, 10, TENANT_ID).onComplete(getAfter -> {
            if (getAfter.failed()) {
              context.fail(getAfter.cause());
            }
            context.assertEquals(0, getAfter.result().getTotalRecords());
            SnapshotDaoUtil.findById(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshotId).onComplete(getSnapshot -> {
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
          recordDao.getRecordByCondition(RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId)), TENANT_ID).onComplete(getNewRecord -> {
            if (getNewRecord.failed()) {
              context.fail(getNewRecord.cause());
            }
            context.assertTrue(getNewRecord.result().isPresent());
            context.assertEquals(State.ACTUAL, getNewRecord.result().get().getState());
            context.assertEquals(expected.getGeneration() + 1, getNewRecord.result().get().getGeneration());
            recordDao.getRecordByCondition(RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(expected.getSnapshotId())), TENANT_ID).onComplete(getOldRecord -> {
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

  private CompositeFuture saveRecords(List<Record> records, String tenantId) {
    return CompositeFuture.all(records.stream().map(record -> {
      return recordService.saveRecord(record, tenantId);
    }).collect(Collectors.toList()));
  }

  private void compareRecords(TestContext context, List<Record> expected, List<Record> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      compareRecords(context, expected.get(i), expected.get(i));
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
      context.assertNull(actual.getRawRecord());
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

  private void compareSourceRecords(TestContext context, List<SourceRecord> expected, List<SourceRecord> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      compareSourceRecords(context, expected.get(i), expected.get(i));
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
    for (int i = 0; i < expected.size(); i++) {
      compareParsedRecords(context, expected.get(i), expected.get(i));
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
    context.assertEquals((String) expected.getContent(), (String) actual.getContent());
    context.assertEquals(expected.getDescription(), actual.getDescription());
  }

  private void compareAdditionalInfo(TestContext context, AdditionalInfo expected, AdditionalInfo actual) {
    context.assertEquals(expected.getSuppressDiscovery(), actual.getSuppressDiscovery());
  }

  private void compareExternalIdsHolder(TestContext context, ExternalIdsHolder expected, ExternalIdsHolder actual) {
    context.assertEquals(expected.getInstanceId(), actual.getInstanceId());
  }

}
