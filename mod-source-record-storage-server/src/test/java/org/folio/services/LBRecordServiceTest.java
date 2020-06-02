package org.folio.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBRecordDaoImpl;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.LBRecordDaoUtil;
import org.folio.dao.util.LBSnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jooq.Tables;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.CompositeFuture;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractLBServiceTest {

  private LBRecordDao recordDao;

  private LBRecordService recordService;

  @Before
  public void setUp(TestContext context) {
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
    recordService = new LBRecordServiceImpl(recordDao);
    Async async = context.async();
    LBSnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots()).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    LBSnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldGetRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    LBRecordDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), records).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      String snapshotId = "ee561342-3098-47a8-ab6e-0f3eba120b04";
      Condition condition = Tables.RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
      List<OrderField<?>> orderFields = new ArrayList<>();
      orderFields.add(Tables.RECORDS_LB.ORDER_IN_FILE.sort(SortOrder.ASC));
      recordService.getRecords(condition, orderFields, 1, 2, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<Record> expected = records.stream()
          .filter(r -> r.getSnapshotId().equals(snapshotId))
          .collect(Collectors.toList());
        context.assertEquals(expected.size(), get.result().getTotalRecords());
        compareRecords(context, TestMocks.getRecord("be1b25ae-4a9d-4077-93e6-7f8e59efd609").get(), get.result().getRecords().get(0));
        compareRecords(context, TestMocks.getRecord("d3cd3e1e-a18c-4f7c-b053-9aa50343394e").get(), get.result().getRecords().get(0));
      });
    });
    async.complete();
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
        context.assertNull(get.result().get().getErrorRecord());
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
  public void shouldSaveRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertNotNull(save.result().getRawRecord());
      context.assertNotNull(save.result().getParsedRecord());
      context.assertNull(save.result().getErrorRecord());
      compareRecords(context, expected, save.result());
      recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertNull(get.result().get().getErrorRecord());
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
  public void shouldSaveRecords(TestContext context) {
    Async async = context.async();
    List<Record> expected = TestMocks.getRecords();
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
      LBRecordDaoUtil.countByCondition(postgresClientFactory.getQueryExecutor(TENANT_ID), DSL.trueCondition()).onComplete(count -> {
        if (count.failed()) {
          context.fail(count.cause());
        }
        context.assertEquals(new Integer(expected.size()), count.result());
        async.complete();
      });
    });
  }

  // TODO: test save records with expected errors

  @Test
  public void shouldUpdateRecord(TestContext context) {
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
        context.assertNotNull(update.result().getRawRecord());
        context.assertNotNull(update.result().getParsedRecord());
        context.assertNull(update.result().getErrorRecord());
        compareRecords(context, expected, update.result());
        recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
          if (get.failed()) {
            context.fail(get.cause());
          }
          // NOTE: getRecordById has implicit condition of state == OLD
          context.assertFalse(get.result().isPresent());
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
  public void shouldGetSourceRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    recordService.saveRecords(recordCollection, TENANT_ID).onComplete(batch -> {
      if (batch.failed()) {
        context.fail(batch.cause());
      }
      Condition condition = DSL.trueCondition();
      List<OrderField<?>> orderFields = new ArrayList<>();
      recordService.getSourceRecords(condition, orderFields, 0, 10, TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        List<SourceRecord> expected = records.stream()
          .map(LBRecordDaoUtil::toSourceRecord)
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
      recordService.getSourceRecordById(expected.getExternalIdsHolder().getInstanceId(), "INSTANCE", TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        compareSourceRecords(context, LBRecordDaoUtil.toSourceRecord(expected), get.result().get());
        async.complete();
      });
    });
  }

  // TODO: test get by matched id not equal to id

  @Test
  public void shouldNotGetSourceRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.getSourceRecordById(expected.getExternalIdsHolder().getInstanceId(), "INSTANCE", TENANT_ID).onComplete(get -> {
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
    recordService.saveRecords(recordCollection, TENANT_ID).onComplete(batch -> {
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
  public void shouldGetFormattedRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getFormattedRecord("INSTANCE", expected.getExternalIdsHolder().getInstanceId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertNotNull(get.result().getParsedRecord());
        context.assertEquals(expected.getParsedRecord().getFormattedContent(), get.result().getParsedRecord().getFormattedContent());
        async.complete();
      });
    });
  }

  private void compareRecords(TestContext context, List<Record> expected, List<Record> actual) {
    context.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      compareRecords(context, expected.get(i), expected.get(i));
    }
  }

  private void compareRecords(TestContext context, Record expected, Record actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getMatchedId(), actual.getMatchedId());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    context.assertEquals(expected.getState(), actual.getState());
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

  private void compareAdditionalInfo(TestContext context, AdditionalInfo expected, AdditionalInfo actual) {
    context.assertEquals(expected.getSuppressDiscovery(), actual.getSuppressDiscovery());
  }

  private void compareExternalIdsHolder(TestContext context, ExternalIdsHolder expected, ExternalIdsHolder actual) {
    context.assertEquals(expected.getInstanceId(), actual.getInstanceId());
  }

  private void compareRawRecords(TestContext context, RawRecord expected, RawRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
  }

  private void compareParsedRecords(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
  }

  private void compareErrorRecords(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
    context.assertEquals(expected.getDescription(), actual.getDescription());
  }

}