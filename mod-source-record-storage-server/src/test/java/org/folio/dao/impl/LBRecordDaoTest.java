package org.folio.dao.impl;

import static java.lang.String.format;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBRecordMocks;
import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordDaoTest extends AbstractEntityDaoTest<Record, RecordCollection, RecordQuery, LBRecordDao, LBRecordMocks> {

  private LBSnapshotDao snapshotDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    dao = new LBRecordDaoImpl();
    FieldUtils.writeField(dao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentEntities(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    snapshotDao.save(TestMocks.getSnapshots(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String sql = format(DELETE_SQL_TEMPLATE, dao.getTableName());
    pgClient.execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      String snapshotSql = format(DELETE_SQL_TEMPLATE, snapshotDao.getTableName());
      pgClient.execute(snapshotSql, snapshotDelete -> {
        if (snapshotDelete.failed()) {
          context.fail(snapshotDelete.cause());
        }
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveGeneratingId(TestContext context) {
    Async async = context.async();
    Record mockRecordWithoutId = getMockRecordWithoutId();
    dao.save(mockRecordWithoutId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, mockRecordWithoutId, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetByMatchedId(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByMatchedId(mocks.getMockEntity().getMatchedId(), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        mocks.compareEntities(context, mocks.getExpectedEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByInstanceId(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      String instanceId = mocks.getMockEntity().getExternalIdsHolder().getInstanceId();
      dao.getByInstanceId(instanceId, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        mocks.compareEntities(context, mocks.getExpectedEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldCalculateGeneration(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntities(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.calculateGeneration(mocks.getMockEntity(), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertEquals(new Integer(0), res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetRecordByInstanceId(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      String instanceId = mocks.getMockEntity().getExternalIdsHolder().getInstanceId();
      dao.getRecordById(instanceId, IncomingIdType.INSTANCE, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        mocks.compareEntities(context, mocks.getExpectedEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetRecordById(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      String id = mocks.getMockEntity().getId();
      dao.getRecordById(id, null, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        mocks.compareEntities(context, mocks.getExpectedEntity(), res.result().get());
        async.complete();
      });
    });
  }

  public Record getMockRecordWithoutId() {
    return new Record()
      .withSnapshotId(TestMocks.getSnapshot(1).getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
      .withOrder(0)
      .withGeneration(0)
      .withState(Record.State.ACTUAL);
  }

  @Override
  public LBRecordMocks initMocks() {
    return LBRecordMocks.mock();
  }

}