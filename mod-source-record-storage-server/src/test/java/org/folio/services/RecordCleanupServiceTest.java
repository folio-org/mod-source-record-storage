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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.jaxrs.model.Record.State.DELETED;
import static org.folio.rest.jaxrs.model.Record.State.OLD;

/**
 * The test creates multiple related records and stores them to DB
 * The related records are:
 * 1) DELETED record generation1 <-- OLD record generation0 (references to DELETED record)
 * 2) ACTUAL record generation1  <-- OLD record generation0 (references to ACTUAL record)
 */
@RunWith(VertxUnitRunner.class)
public class RecordCleanupServiceTest extends AbstractLBServiceTest {
  private final RecordDao recordDao = new RecordDaoImpl(postgresClientFactory);
  private final RecordService recordService = new RecordServiceImpl(recordDao);
  private final Snapshot snapshot;
  private final Record deletedRecord;
  private final Record oldRecordForDeletedRecord;
  private final Record actualRecord;
  private final Record oldRecordForActualRecord;

  public RecordCleanupServiceTest() {
    this.snapshot = TestMocks.getSnapshot(0);
    String deletedRecordId = UUID.randomUUID().toString();
    this.deletedRecord = new Record()
      .withId(deletedRecordId)
      .withState(DELETED)
      .withMatchedId(deletedRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(1)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withDeleted(true)
      .withOrder(1)
      .withExternalIdsHolder(new ExternalIdsHolder())
      .withLeaderRecordStatus("n")
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(deletedRecordId))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(deletedRecordId))
      .withErrorRecord(TestMocks.getRecord(0).getErrorRecord().withId(deletedRecordId));
    String oldRecordForDeletedRecordId = UUID.randomUUID().toString();
    this.oldRecordForDeletedRecord = new Record()
      .withId(oldRecordForDeletedRecordId)
      .withState(OLD)
      .withMatchedId(deletedRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withDeleted(false)
      .withOrder(0)
      .withExternalIdsHolder(new ExternalIdsHolder())
      .withLeaderRecordStatus("n")
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(oldRecordForDeletedRecordId))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(oldRecordForDeletedRecordId))
      .withErrorRecord(TestMocks.getRecord(0).getErrorRecord().withId(oldRecordForDeletedRecordId));
    String actualRecordId = UUID.randomUUID().toString();
    this.actualRecord = new Record()
      .withId(actualRecordId)
      .withState(ACTUAL)
      .withMatchedId(actualRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(1)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withDeleted(false)
      .withOrder(1)
      .withExternalIdsHolder(new ExternalIdsHolder())
      .withLeaderRecordStatus("n")
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(actualRecordId))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(actualRecordId))
      .withErrorRecord(TestMocks.getRecord(0).getErrorRecord().withId(actualRecordId));
    String oldRecordForActualRecordId = UUID.randomUUID().toString();
    this.oldRecordForActualRecord = new Record()
      .withId(oldRecordForActualRecordId)
      .withState(ACTUAL)
      .withMatchedId(actualRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withDeleted(false)
      .withOrder(0)
      .withExternalIdsHolder(new ExternalIdsHolder())
      .withLeaderRecordStatus("n")
      .withRawRecord(TestMocks.getRecord(0).getRawRecord().withId(oldRecordForActualRecordId))
      .withParsedRecord(TestMocks.getRecord(0).getParsedRecord().withId(oldRecordForActualRecordId))
      .withErrorRecord(TestMocks.getRecord(0).getErrorRecord().withId(oldRecordForActualRecordId));
  }

  @Before
  public void before(TestContext testContext) throws InterruptedException {
    Async async = testContext.async();
    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    SnapshotDaoUtil.save(queryExecutor, snapshot)
      .compose(ar -> recordService.saveRecord(deletedRecord, TENANT_ID))
      .compose(ar -> recordService.saveRecord(oldRecordForDeletedRecord, TENANT_ID))
      .compose(ar -> recordService.saveRecord(actualRecord, TENANT_ID))
      .compose(ar -> recordService.saveRecord(oldRecordForActualRecord, TENANT_ID))
      .onSuccess(ar -> async.complete());
  }

  @After
  public void after(TestContext testContext) throws InterruptedException {
    Async async = testContext.async();
    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    SnapshotDaoUtil.delete(queryExecutor, snapshot.getJobExecutionId())
      .compose(ar -> recordService.deleteRecordsBySnapshotId(snapshot.getJobExecutionId(), TENANT_ID))
      .onSuccess(ar -> async.complete());
  }

  /* The test verifies whether the DELETED record, and it's related OLD record are purged from DB when cleanup is done */
  @Test
  public void shouldPurge_DELETED_and_OLD_records(TestContext context) {
    // given
    Async async = context.async();
    RecordCleanupService recordCleanupService = new RecordCleanupServiceImpl(recordDao, 100);
    // when
    long timerId = recordCleanupService.initialize(vertx, TENANT_ID);
    // then
    vertx.setTimer(1_000, timerHandler -> CompositeFuture.all(
          verifyRecordPurged(deletedRecord.getId(), context),
          verifyRecordPurged(oldRecordForDeletedRecord.getId(), context)
        )
        .onSuccess(verifyRecordPurged -> {
          vertx.cancelTimer(timerId);
          async.complete();
        })
        .onFailure(context::fail)
    );
  }

  /*
      The test verifies whether the DELETED record, and its related OLD record are stay in DB.
      The records should stay in DB because of default cleanup delay is long enough to start cleanup process.
  */
  @Test
  public void shouldNotPurge_DELETED_and_OLD_records(TestContext context) {
    // given
    Async async = context.async();
    RecordCleanupService recordCleanupService = new RecordCleanupServiceImpl(recordDao);
    // when
    long timerId = recordCleanupService.initialize(vertx, TENANT_ID);
    // then
    CompositeFuture.all(
        verifyRecordIsPresent(deletedRecord.getId(), context),
        verifyRecordIsPresent(oldRecordForDeletedRecord.getId(), context)
      )
      .onSuccess(verifyRecordIsPresent -> {
        vertx.cancelTimer(timerId);
        async.complete();
      })
      .onFailure(context::fail);
  }

  /* The test verifies whether the ACTUAL record, and it's related OLD are stay in DB when cleanup is done */
  @Test
  public void shouldNotPurge_ACTUAL_and_OLD_records(TestContext context) {
    // given
    Async async = context.async();
    RecordCleanupService recordCleanupService = new RecordCleanupServiceImpl(recordDao, 100);
    // when
    long timerId = recordCleanupService.initialize(vertx, TENANT_ID);
    // then
    vertx.setTimer(1_000, timerHandler -> CompositeFuture.all(
          verifyRecordIsPresent(actualRecord.getId(), context),
          verifyRecordIsPresent(oldRecordForActualRecord.getId(), context)
        )
        .onSuccess(verifyRecordIsPresent -> {
          vertx.cancelTimer(timerId);
          async.complete();
        })
        .onFailure(context::fail)
    );
  }

  private Future<Void> verifyRecordPurged(String recordId, TestContext testContext) {
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
