package org.folio.services;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.TestMocks;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.ErrorRecordDaoUtil;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RawRecordDaoUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.cleanup.RecordCleanupService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.folio.rest.jaxrs.model.Record.State.DELETED;

/**
 * The test creates a several related records and stores them to DB
 * The records are:
 * DELETED record generation1 <-- DELETED record generation0 (references to DELETED record)
 */
@RunWith(VertxUnitRunner.class)
public class RecordCleanupServiceTest extends AbstractLBServiceTest {
  private final RecordDao recordDao = new RecordDaoImpl(postgresClientFactory);
  private final RecordService recordService = new RecordServiceImpl(recordDao);
  private final Snapshot snapshot;
  private final Record deletedRecordGen1;
  private final Record deletedRecordGen0;

  public RecordCleanupServiceTest() {
    this.snapshot = TestMocks.getSnapshot(0);
    String deletedRecordGen1Id = UUID.randomUUID().toString();
    this.deletedRecordGen1 = new Record()
      .withId(deletedRecordGen1Id)
      .withState(DELETED)
      .withMatchedId(deletedRecordGen1Id)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(1)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withDeleted(true)
      .withOrder(1)
      .withExternalIdsHolder(new ExternalIdsHolder())
      .withLeaderRecordStatus("n")
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(deletedRecordGen1Id))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(deletedRecordGen1Id))
      .withErrorRecord(TestMocks.getRecord(0).getErrorRecord().withId(deletedRecordGen1Id));
    String deletedRecordGen0Id = UUID.randomUUID().toString();
    this.deletedRecordGen0 = new Record()
      .withId(deletedRecordGen0Id)
      .withState(DELETED)
      .withMatchedId(deletedRecordGen1Id)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withDeleted(false)
      .withOrder(0)
      .withExternalIdsHolder(new ExternalIdsHolder())
      .withLeaderRecordStatus("n")
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(deletedRecordGen0Id))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(deletedRecordGen0Id))
      .withErrorRecord(TestMocks.getRecord(0).getErrorRecord().withId(deletedRecordGen0Id));
  }

  @Before
  public void before(TestContext context) {
    Async async = context.async();
    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    SnapshotDaoUtil.save(queryExecutor, snapshot)
      .compose(ar -> recordService.saveRecord(deletedRecordGen1, TENANT_ID))
      .compose(ar -> recordService.saveRecord(deletedRecordGen0, TENANT_ID))
      .onComplete(ar -> async.complete())
      .onFailure(throwable -> context.fail(throwable));
  }

  @After
  public void after(TestContext testContext) throws InterruptedException {
    Async async = testContext.async();
    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    SnapshotDaoUtil.delete(queryExecutor, snapshot.getJobExecutionId())
      .compose(ar -> recordService.deleteRecordsBySnapshotId(snapshot.getJobExecutionId(), TENANT_ID))
      .onComplete(ar -> async.complete());
  }

  /*
      The test verifies whether the records are purged from DB when cleanup is done;
      If the 'limit' = 0, it means to delete all the records
  */
  @Test
  public void shouldPurgeRecords_limitIs0(TestContext context) {
    // given
    Async async = context.async();
    RecordCleanupService service = new RecordCleanupService(0, 0, vertx, recordDao);
    // when
    service.cleanup();
    // then
    vertx.setTimer(1_000, timerHandler -> CompositeFuture.all(
          verifyRecordIsPurged(deletedRecordGen1.getId(), context),
          verifyRecordIsPurged(deletedRecordGen0.getId(), context)
        )
        .onSuccess(ar -> async.complete())
        .onFailure(context::fail)
    );
  }

  /*
      The test verifies whether the DELETED record generation1 is purged, and its related record generation0 is stay in DB when cleanup is done.
      The record generation0 is present in DB after cleanup because 'limit' = 1, and the record was created later than record of generation1.
  */
  @Test
  public void shouldPurgeRecord_limitIs1(TestContext context) {
    // given
    Async async = context.async();
    RecordCleanupService service = new RecordCleanupService(0, 1, vertx, recordDao);
    // when
    service.cleanup();
    // then
    vertx.setTimer(1_000, timerHandler -> CompositeFuture.all(
          verifyRecordIsPurged(deletedRecordGen1.getId(), context),
          verifyRecordIsPresent(deletedRecordGen0.getId(), context)
        )
        .onSuccess(ar -> async.complete())
        .onFailure(context::fail)
    );
  }

  /*
      The test verifies whether the records are stay in DB when cleanup is done, because 'lastUpdatedDays' = 10 means
      the only records updated more than 10 are getting purged.
  */
  @Test
  public void shouldNotPurgeRecords_lastUpdatedDaysIs10(TestContext context) {
    // given
    Async async = context.async();
    RecordCleanupService service = new RecordCleanupService(10, 2, vertx, recordDao);
    // when
    service.cleanup();
    // then
    vertx.setTimer(1_000, timerHandler -> CompositeFuture.all(
          verifyRecordIsPresent(deletedRecordGen1.getId(), context),
          verifyRecordIsPresent(deletedRecordGen0.getId(), context)
        )
        .onSuccess(ar -> async.complete())
        .onFailure(context::fail)
    );
  }

  private Future<Void> verifyRecordIsPurged(String recordId, TestContext testContext) {
    Promise<Void> promise = Promise.promise();
    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    Future.succeededFuture()
      // verification
      .compose(ar -> RecordDaoUtil.findById(queryExecutor, recordId))
      .onSuccess(optionalRecord -> testContext.assertTrue(optionalRecord.isEmpty()))
      .compose(ar -> ParsedRecordDaoUtil.findById(queryExecutor, recordId, RecordType.MARC_BIB))
      .onSuccess(optionalParsedRecord -> testContext.assertTrue(optionalParsedRecord.isEmpty()))
      .compose(ar -> RawRecordDaoUtil.findById(queryExecutor, recordId))
      .onSuccess(optionalRawRecord -> testContext.assertTrue(optionalRawRecord.isEmpty()))
      .compose(ar -> ErrorRecordDaoUtil.findById(queryExecutor, recordId))
      .onSuccess(optionalErrorRecord -> testContext.assertTrue(optionalErrorRecord.isEmpty()))
      // handling complete
      .onSuccess(ar -> promise.complete())
      .onFailure(ar -> promise.fail(ar));
    return promise.future();
  }

  private Future<Void> verifyRecordIsPresent(String recordId, TestContext testContext) {
    Promise<Void> promise = Promise.promise();
    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    Future.succeededFuture()
      // verification
      .compose(ar -> RecordDaoUtil.findById(queryExecutor, recordId))
      .onSuccess(optionalRecord -> testContext.assertTrue(optionalRecord.isPresent()))
      .compose(ar -> ParsedRecordDaoUtil.findById(queryExecutor, recordId, RecordType.MARC_BIB))
      .onSuccess(optionalParsedRecord -> testContext.assertTrue(optionalParsedRecord.isPresent()))
      .compose(ar -> RawRecordDaoUtil.findById(queryExecutor, recordId))
      .onSuccess(optionalRawRecord -> testContext.assertTrue(optionalRawRecord.isPresent()))
      .compose(ar -> ErrorRecordDaoUtil.findById(queryExecutor, recordId))
      .onSuccess(optionalErrorRecord -> testContext.assertTrue(optionalErrorRecord.isPresent()))
      // handling complete
      .onSuccess(ar -> promise.complete())
      .onFailure(ar -> promise.fail(ar));
    return promise.future();
  }
}
