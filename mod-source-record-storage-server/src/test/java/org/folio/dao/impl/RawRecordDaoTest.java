package org.folio.dao.impl;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.query.RawRecordQuery;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanUtils;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RawRecordDaoTest extends AbstractEntityDaoTest<RawRecord, RawRecordCollection, RawRecordQuery, RawRecordDao> {

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    recordDao = new LBRecordDaoImpl();
    FieldUtils.writeField(recordDao, "postgresClientFactory", postgresClientFactory, true);
    dao = new RawRecordDaoImpl();
    FieldUtils.writeField(dao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentEntities(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    snapshotDao.save(getSnapshots(), TENANT_ID).setHandler(saveSnapshots -> {
      if (saveSnapshots.failed()) {
        context.fail(saveSnapshots.cause());
      }
      recordDao.save(getRecords(), TENANT_ID).setHandler(saveRecords -> {
        if (saveRecords.failed()) {
          context.fail(saveRecords.cause());
        }
        async.complete();
      });
    });
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

  @Override
  public RawRecordQuery getNoopQuery() {
    return new RawRecordQuery();
  }

  @Override
  public RawRecordQuery getArbitruaryQuery() {
    RawRecordQuery snapshotQuery = new RawRecordQuery();
    // NOTE: no reasonable field to filter on
    return snapshotQuery;
  }

  @Override
  public RawRecordQuery getArbitruarySortedQuery() {
    return (RawRecordQuery) getArbitruaryQuery()
      .orderBy("id");
  }

  @Override
  public RawRecord getMockEntity() {
    return getRawRecord(0);
  }

  @Override
  public RawRecord getInvalidMockEntity() {
    return new RawRecord()
      .withId(getRecord(0).getId());
  }

  @Override
  public RawRecord getUpdatedMockEntity() {
    return new RawRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent());
  }

  @Override
  public List<RawRecord> getMockEntities() {
    return getRawRecords();
  }

  @Override
  public void compareEntities(TestContext context, RawRecord expected, RawRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
  }

  @Override
  public void assertNoopQueryResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  @Override
  public void assertArbitruaryQueryResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  @Override
  public void assertArbitruarySortedQueryResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    Collections.sort(expected, (rr1, rr2) -> rr1.getId().compareTo(rr2.getId()));
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  @Override
  public RawRecordQuery getCompleteQuery() {
    RawRecordQuery query = new RawRecordQuery();
    BeanUtils.copyProperties(getRawRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    return query;
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'";
  }

}