package org.folio.dao.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBSnapshotMocks;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBSnapshotDaoTest extends AbstractEntityDaoTest<Snapshot, SnapshotCollection, SnapshotQuery, LBSnapshotDao, LBSnapshotMocks> {

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    dao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(dao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentEntities(TestContext context) {
    // NOTE: no dependent entities needed for testing Snapshot DAO
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String sql = String.format(DELETE_SQL_TEMPLATE, dao.getTableName());
    pgClient.execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldSaveGeneratingId(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntityWithoutId(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, getMockEntityWithoutId(), res.result());
      async.complete();
    });
  }

  public Snapshot getMockEntityWithoutId() {
    return new Snapshot()
      .withStatus(Snapshot.Status.NEW);
  }

  @Override
  public LBSnapshotMocks initMocks() {
    return LBSnapshotMocks.mock();
  }

}