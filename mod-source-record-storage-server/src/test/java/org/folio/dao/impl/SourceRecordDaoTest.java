package org.folio.dao.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SourceRecordDaoTest extends AbstractDaoTest {

  private SourceRecordDao sourceRecordDao;

  private LBSnapshotDao snapshotDao;
  private LBRecordDao recordDao;
  private RawRecordDao rawRecordDao;
  private ParsedRecordDao parsedRecordDao;
  private ErrorRecordDao errorRecordDao;

  private Record mockRecord;
  private Record[] mockRecords;

  private RawRecord mockRawRecord;
  private RawRecord[] mockRawRecords;

  private ParsedRecord mockParsedRecord;
  private ParsedRecord[] mockParsedRecords;

  private ErrorRecord mockErrorRecord;
  private ErrorRecord[] mockErrorRecords;

  @Override
  public void createDao(TestContext context) {
    sourceRecordDao = new SourceRecordDaoImpl(postgresClientFactory);
  }

  @Override
  public void createDependentBeans(TestContext context) {
    snapshotDao = new LBSnapshotDaoImpl(postgresClientFactory);
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
    rawRecordDao = new RawRecordDaoImpl(postgresClientFactory);
    parsedRecordDao = new ParsedRecordDaoImpl(postgresClientFactory);
    errorRecordDao = new ErrorRecordDaoImpl(postgresClientFactory);
    Async async = context.async();
    createRecords(context).setHandler(createRecords -> {
      if (createRecords.failed()) {
        context.fail();
      }
      CompositeFuture.all(
        createRawRecords(context),
        createParsedRecords(context),
        createErrorRecords(context)
      ).setHandler(create -> {
        if (create.failed()) {
          context.fail();
        }
        async.complete();
      });
    });
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String rawRecordSql = String.format(DELETE_SQL_TEMPLATE, rawRecordDao.getTableName());
    String parsedRecordSql = String.format(DELETE_SQL_TEMPLATE, parsedRecordDao.getTableName());
    String errorRecordSql = String.format(DELETE_SQL_TEMPLATE, errorRecordDao.getTableName());

    Promise<AsyncResult<UpdateResult>> rawRecordDeletePromise = Promise.promise();
    Promise<AsyncResult<UpdateResult>> parsedRecordDeletePromise = Promise.promise();
    Promise<AsyncResult<UpdateResult>> errorRecordDeletePromise = Promise.promise();
    pgClient.execute(rawRecordSql, deleteHandler(rawRecordDeletePromise));
    pgClient.execute(parsedRecordSql, deleteHandler(parsedRecordDeletePromise));
    pgClient.execute(errorRecordSql, deleteHandler(errorRecordDeletePromise));
    CompositeFuture.all(
      rawRecordDeletePromise.future(),
      parsedRecordDeletePromise.future(),
      errorRecordDeletePromise.future()
    ).setHandler(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      String recordSql = String.format(DELETE_SQL_TEMPLATE, recordDao.getTableName());
      pgClient.execute(recordSql, recordDelete -> {
        if (recordDelete.failed()) {
          context.fail(recordDelete.cause());
        }
        String snapshotSql = String.format(DELETE_SQL_TEMPLATE, snapshotDao.getTableName());
        pgClient.execute(snapshotSql, snapshotDelete -> {
          if (snapshotDelete.failed()) {
            context.fail(snapshotDelete.cause());
          }
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldGetSourceMarcRecordById(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordById(mockRecord.getId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(mockRecord.getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) mockParsedRecord.getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByIdAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordByIdAlt(mockRecords[0].getId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(mockRecords[0].getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) mockParsedRecords[2].getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceId(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordByInstanceId(mockRecord.getExternalIdsHolder().getInstanceId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(mockRecord.getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) mockParsedRecord.getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceIdAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordByInstanceIdAlt(mockRecords[3].getExternalIdsHolder().getInstanceId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(mockRecords[3].getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) mockParsedRecords[4].getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecords(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecords(0, 10, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }

      List<Record> allMockRecords = new ArrayList<>(Arrays.asList(mockRecords));
      allMockRecords.add(mockRecord);

      List<Record> expectedMockRecords = allMockRecords.stream()
        .filter(mockRecord -> mockRecord.getState().equals(State.ACTUAL))
        .collect(Collectors.toList());

      context.assertEquals(expectedMockRecords.size(), res.result().getTotalRecords());

      async.complete();
    });
  }

  private Handler<AsyncResult<UpdateResult>> deleteHandler(Promise<AsyncResult<UpdateResult>> promise) {
    return delete -> {
      if (delete.failed()) {
        promise.fail(delete.cause());
        return;
      }
      promise.complete(delete);
    };
  }

  private Future<Boolean> createRecords(TestContext context) {
    Promise<Boolean> promise = Promise.promise();
    Snapshot[] snapshots = MockSnapshotFactory.getMockSnapshots();
    CompositeFuture.all(
      snapshotDao.save(snapshots[0], TENANT_ID),
      snapshotDao.save(snapshots[1], TENANT_ID)
    ).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      Snapshot snapshot1 = save.result().resultAt(0);
      Snapshot snapshot2 = save.result().resultAt(1);

      mockRecord = MockRecordFactory.getMockRecord(snapshot1);
      mockRecords = MockRecordFactory.getMockRecords(snapshot1, snapshot2);

      recordDao.save(mockRecord, TENANT_ID).setHandler(saveRecord -> {
        if (saveRecord.failed()) {
          context.fail(saveRecord.cause());
        }
        mockRecord = saveRecord.result();

        CompositeFuture.all(
          recordDao.save(mockRecords[0], TENANT_ID),
          recordDao.save(mockRecords[1], TENANT_ID),
          recordDao.save(mockRecords[2], TENANT_ID),
          recordDao.save(mockRecords[3], TENANT_ID),
          recordDao.save(mockRecords[4], TENANT_ID)
        ).setHandler(saveRecords -> {
          if (saveRecords.failed()) {
            context.fail(saveRecords.cause());
          }
  
          mockRecords[0] = saveRecords.result().resultAt(0);
          mockRecords[1] = saveRecords.result().resultAt(1);
          mockRecords[2] = saveRecords.result().resultAt(2);
          mockRecords[3] = saveRecords.result().resultAt(3);
          mockRecords[4] = saveRecords.result().resultAt(4);
  
          promise.complete(true);
        });
      });
    });
    return promise.future();
  }

  private Future<Boolean> createRawRecords(TestContext context) {
    Promise<Boolean> promise = Promise.promise();
    mockRawRecord = MockRawRecordFactory.getMockRawRecord(mockRecord);
    rawRecordDao.save(mockRawRecord, TENANT_ID).setHandler(save -> {
      mockRawRecords = MockRawRecordFactory.getMockRawRecords(mockRecords);
      CompositeFuture.all(
        rawRecordDao.save(mockRawRecords[0], TENANT_ID),
        rawRecordDao.save(mockRawRecords[1], TENANT_ID),
        rawRecordDao.save(mockRawRecords[2], TENANT_ID),
        rawRecordDao.save(mockRawRecords[3], TENANT_ID),
        rawRecordDao.save(mockRawRecords[4], TENANT_ID)
        ).setHandler(saveRawRecords -> {
          if (saveRawRecords.failed()) {
            context.fail(saveRawRecords.cause());
          }
          mockRawRecords[0] = saveRawRecords.result().resultAt(0);
          mockRawRecords[1] = saveRawRecords.result().resultAt(1);
          mockRawRecords[2] = saveRawRecords.result().resultAt(2);
          mockRawRecords[3] = saveRawRecords.result().resultAt(3);
          mockRawRecords[4] = saveRawRecords.result().resultAt(4);
            promise.complete(true);
        });
    });
    return promise.future();
  }

  private Future<Boolean> createParsedRecords(TestContext context) {
    Promise<Boolean> promise = Promise.promise();
    mockParsedRecord = MockParsedRecordFactory.getMockParsedRecord(mockRecord);
    parsedRecordDao.save(mockParsedRecord, TENANT_ID).setHandler(save -> {
      mockParsedRecords = MockParsedRecordFactory.getMockParsedRecords(mockRecords);
      CompositeFuture.all(
        parsedRecordDao.save(mockParsedRecords[0], TENANT_ID),
        parsedRecordDao.save(mockParsedRecords[1], TENANT_ID),
        parsedRecordDao.save(mockParsedRecords[2], TENANT_ID),
        parsedRecordDao.save(mockParsedRecords[3], TENANT_ID),
        parsedRecordDao.save(mockParsedRecords[4], TENANT_ID)
        ).setHandler(saveParsedRecords -> {
          if (saveParsedRecords.failed()) {
            context.fail(saveParsedRecords.cause());
          }
          mockParsedRecords[0] = saveParsedRecords.result().resultAt(0);
          mockParsedRecords[1] = saveParsedRecords.result().resultAt(1);
          mockParsedRecords[2] = saveParsedRecords.result().resultAt(2);
          mockParsedRecords[3] = saveParsedRecords.result().resultAt(3);
          mockParsedRecords[4] = saveParsedRecords.result().resultAt(4);
            promise.complete(true);
        });
    });
    return promise.future();
  }

  private Future<Boolean> createErrorRecords(TestContext context) {
    Promise<Boolean> promise = Promise.promise();
    mockErrorRecord = MockErrorRecordFactory.getMockErrorRecord(mockRecord);
    errorRecordDao.save(mockErrorRecord, TENANT_ID).setHandler(save -> {
      mockErrorRecords = MockErrorRecordFactory.getMockErrorRecords(mockRecords);
      CompositeFuture.all(
        errorRecordDao.save(mockErrorRecords[0], TENANT_ID),
        errorRecordDao.save(mockErrorRecords[1], TENANT_ID),
        errorRecordDao.save(mockErrorRecords[2], TENANT_ID),
        errorRecordDao.save(mockErrorRecords[3], TENANT_ID),
        errorRecordDao.save(mockErrorRecords[4], TENANT_ID)
        ).setHandler(saveErrorRecords -> {
          if (saveErrorRecords.failed()) {
            context.fail(saveErrorRecords.cause());
          }
          mockErrorRecords[0] = saveErrorRecords.result().resultAt(0);
          mockErrorRecords[1] = saveErrorRecords.result().resultAt(1);
          mockErrorRecords[2] = saveErrorRecords.result().resultAt(2);
          mockErrorRecords[3] = saveErrorRecords.result().resultAt(3);
          mockErrorRecords[4] = saveErrorRecords.result().resultAt(4);
            promise.complete(true);
        });
    });
    return promise.future();
  }

}