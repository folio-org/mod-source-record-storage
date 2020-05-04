package org.folio.dao.impl;

import java.util.List;

import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.filter.ParsedRecordFilter;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordDaoTest extends AbstractBeanDaoTest<ParsedRecord, ParsedRecordCollection, ParsedRecordFilter, ParsedRecordDao> {

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
    dao = new ParsedRecordDaoImpl(postgresClientFactory);
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
  public ParsedRecordFilter getNoopFilter() {
    return new ParsedRecordFilter();
  }

  @Override
  public ParsedRecordFilter getArbitruaryFilter() {
    ParsedRecordFilter snapshotFilter = new ParsedRecordFilter();
    // NOTE: no reasonable field to filter on
    return snapshotFilter;
  }

  @Override
  public ParsedRecord getMockBean() {
    return getParsedRecord(0);
  }

  @Override
  public ParsedRecord getInvalidMockBean() {
    return new ParsedRecord()
      .withId(getRecord(0).getId());
  }

  @Override
  public ParsedRecord getUpdatedMockBean() {
    return new ParsedRecord()
      .withId(getMockBean().getId())
      .withContent(getMockBean().getContent());
  }

  @Override
  public List<ParsedRecord> getMockBeans() {
    return getParsedRecords();
  }

  @Override
  public void compareBeans(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(new JsonObject((String) expected.getContent()),
      new JsonObject((String) actual.getContent()));
    context.assertEquals(expected.getFormattedContent().trim(), actual.getFormattedContent().trim());
  }

  @Override
  public void assertNoopFilterResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockBeans();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockBeans();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

}