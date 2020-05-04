package org.folio.dao.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
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

  @Override
  public void createDao(TestContext context) {
    sourceRecordDao = new SourceRecordDaoImpl(postgresClientFactory);
  }

  @Override
  public void createDependentBeans(TestContext context) {
    Async async = context.async();
    snapshotDao = new LBSnapshotDaoImpl(postgresClientFactory);
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
    rawRecordDao = new RawRecordDaoImpl(postgresClientFactory);
    parsedRecordDao = new ParsedRecordDaoImpl(postgresClientFactory);
    errorRecordDao = new ErrorRecordDaoImpl(postgresClientFactory);
    snapshotDao.save(getSnapshots(), TENANT_ID).setHandler(saveSnapshots -> {
      if (saveSnapshots.failed()) {
        context.fail(saveSnapshots.cause());
      }
      recordDao.save(getRecords(), TENANT_ID).setHandler(saveRecords -> {
        if (saveRecords.failed()) {
          context.fail(saveRecords.cause());
        }
        CompositeFuture.all(
          rawRecordDao.save(getRawRecords(), TENANT_ID),
          parsedRecordDao.save(getParsedRecords(), TENANT_ID),
          errorRecordDao.save(getErrorRecords(), TENANT_ID)
        ).setHandler(save -> {
          if (save.failed()) {
            context.fail();
          }
          async.complete();
        });
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
    sourceRecordDao.getSourceMarcRecordById(getRecord(0).getId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(getRecord(0).getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) getParsedRecord(0).getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByIdAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordByIdAlt(getRecord(0).getId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(getRecord(0).getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) getParsedRecord(0).getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceId(TestContext context) {
    Async async = context.async();
    String instanceId = getRecord(0).getExternalIdsHolder().getInstanceId();
    sourceRecordDao.getSourceMarcRecordByInstanceId(instanceId, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(getRecord(0).getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) getParsedRecord(0).getContent()),
        new JsonObject((String) res.result().get().getParsedRecord().getContent()));
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceIdAlt(TestContext context) {
    Async async = context.async();
    String instanceId = getRecord(0).getExternalIdsHolder().getInstanceId();
    sourceRecordDao.getSourceMarcRecordByInstanceIdAlt(instanceId, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      context.assertEquals(getRecord(0).getId(), res.result().get().getRecordId());
      context.assertEquals(new JsonObject((String) getParsedRecord(0).getContent()),
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

      List<Record> expectedRecords = getRecords().stream()
        .filter(mockRecord -> mockRecord.getState().equals(State.ACTUAL))
        .collect(Collectors.toList());

      context.assertEquals(expectedRecords.size(), res.result().getTotalRecords());

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

}