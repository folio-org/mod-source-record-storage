package org.folio.dao.impl;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.OrderBy.Direction;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanUtils;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordDaoTest extends AbstractEntityDaoTest<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao> {

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    recordDao = new LBRecordDaoImpl();
    FieldUtils.writeField(recordDao, "postgresClientFactory", postgresClientFactory, true);
    dao = new ParsedRecordDaoImpl();
    FieldUtils.writeField(dao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentEntities(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    
    snapshotDao.save(getSnapshots(), TENANT_ID).onComplete(saveSnapshots -> {
      if (saveSnapshots.failed()) {
        context.fail(saveSnapshots.cause());
      }
      recordDao.save(getRecords(), TENANT_ID).onComplete(saveRecords -> {
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
  public ParsedRecordQuery getNoopQuery() {
    return new ParsedRecordQuery();
  }

  @Override
  public ParsedRecordQuery getArbitruaryQuery() {
    ParsedRecordQuery snapshotQuery = new ParsedRecordQuery();
    // NOTE: no reasonable field to filter on
    return snapshotQuery;
  }

  @Override
  public ParsedRecordQuery getArbitruarySortedQuery() {
    return (ParsedRecordQuery) getArbitruaryQuery()
      .orderBy("id", Direction.DESC);
  }

  @Override
  public ParsedRecord getMockEntity() {
    return getParsedRecord(0);
  }

  @Override
  public ParsedRecord getInvalidMockEntity() {
    return new ParsedRecord()
      .withId(getRecord(0).getId());
  }

  @Override
  public ParsedRecord getUpdatedMockEntity() {
    return new ParsedRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent());
  }

  @Override
  public List<ParsedRecord> getMockEntities() {
    return getParsedRecords();
  }

  @Override
  public void compareEntities(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(new JsonObject((String) expected.getContent()),
      new JsonObject((String) actual.getContent()));
    context.assertEquals(expected.getFormattedContent().trim(), actual.getFormattedContent().trim());
  }

  @Override
  public void assertNoopQueryResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  @Override
  public void assertArbitruaryQueryResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  @Override
  public void assertArbitruarySortedQueryResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockEntities();
    Collections.sort(expected, (pr1, pr2) -> pr2.getId().compareTo(pr1.getId()));
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  @Override
  public ParsedRecordQuery getCompleteQuery() {
    ParsedRecordQuery query = new ParsedRecordQuery();
    BeanUtils.copyProperties(getParsedRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    return query;
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'";
  }

}