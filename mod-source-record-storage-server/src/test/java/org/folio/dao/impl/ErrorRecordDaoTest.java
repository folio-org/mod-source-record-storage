package org.folio.dao.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanUtils;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ErrorRecordDaoTest extends AbstractEntityDaoTest<ErrorRecord, ErrorRecordCollection, ErrorRecordFilter, ErrorRecordDao> {

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    recordDao = new LBRecordDaoImpl();
    FieldUtils.writeField(recordDao, "postgresClientFactory", postgresClientFactory, true);
    dao = new ErrorRecordDaoImpl();
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
  public ErrorRecord getMockEntity() {
    return getErrorRecord(0);
  }

  @Override
  public ErrorRecord getInvalidMockEntity() {
    return new ErrorRecord()
      .withId(getRecord(0).getId());
  }

  @Override
  public ErrorRecord getUpdatedMockEntity() {
    return new ErrorRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent())
      .withDescription("Something went really wrong");
  }

  @Override
  public List<ErrorRecord> getMockEntities() {
    return getErrorRecords();
  }

  @Override
  public void compareEntities(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getDescription(), actual.getDescription());
    context.assertEquals(new JsonObject((String) expected.getContent()), new JsonObject((String) actual.getContent()));
  }

  @Override
  public void assertNoopFilterResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockEntities().stream()
      .filter(entity -> entity.getDescription().equals(getArbitruaryFilter().getDescription()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  @Override
  public ErrorRecordFilter getCompleteFilter() {
    ErrorRecordFilter filter = new ErrorRecordFilter();
    BeanUtils.copyProperties(getErrorRecord(0), filter);
    return filter;
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = 'd3cd3e1e-a18c-4f7c-b053-9aa50343394e'" +
      " AND description = 'Opps... something went wrong'";
  }

}