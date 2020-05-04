package org.folio.dao.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ErrorRecordDaoTest extends AbstractBeanDaoTest<ErrorRecord, ErrorRecordCollection, ErrorRecordFilter, ErrorRecordDao> {

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  @Override
  public void createDependentBeans(TestContext context) {
    Async async = context.async();
    snapshotDao = new LBSnapshotDaoImpl(postgresClientFactory);
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
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
  public void createDao(TestContext context) {
    dao = new ErrorRecordDaoImpl(postgresClientFactory);
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
  public ErrorRecordFilter getNoopFilter() {
    return new ErrorRecordFilter();
  }

  @Override
  public ErrorRecordFilter getArbitruaryFilter() {
    ErrorRecordFilter snapshotFilter = new ErrorRecordFilter();
    snapshotFilter.setDescription("Oops... something happened");
    return snapshotFilter;
  }

  @Override
  public ErrorRecord getMockBean() {
    return getErrorRecord(0);
  }

  @Override
  public ErrorRecord getInvalidMockBean() {
    return new ErrorRecord()
      .withId(getRecord(0).getId());
  }

  @Override
  public ErrorRecord getUpdatedMockBean() {
    return new ErrorRecord()
      .withId(getMockBean().getId())
      .withContent(getMockBean().getContent())
      .withDescription("Something went really wrong");
  }

  @Override
  public List<ErrorRecord> getMockBeans() {
    return getErrorRecords();
  }

  @Override
  public void compareBeans(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getDescription(), actual.getDescription());
    context.assertEquals(new JsonObject((String) expected.getContent()), new JsonObject((String) actual.getContent()));
  }

  @Override
  public void assertNoopFilterResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockBeans();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockBeans().stream()
      .filter(bean -> bean.getDescription().equals(getArbitruaryFilter().getDescription()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

}