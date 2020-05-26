package org.folio.services.impl;

import static java.lang.String.format;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.impl.AbstractDaoTest;
import org.folio.dao.impl.LBRecordDaoImpl;
import org.folio.dao.impl.LBSnapshotDaoImpl;
import org.folio.dao.impl.ParsedRecordDaoImpl;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.ParsedRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordServiceDaoTest extends AbstractDaoTest {

  ParsedRecordService parsedRecordService;

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  ParsedRecordDao parsedRecordDao;

  @Override
  public void createBeans(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    recordDao = new LBRecordDaoImpl();
    FieldUtils.writeField(recordDao, "postgresClientFactory", postgresClientFactory, true);
    parsedRecordDao = new ParsedRecordDaoImpl();
    FieldUtils.writeField(parsedRecordDao, "postgresClientFactory", postgresClientFactory, true);

    parsedRecordService = new ParsedRecordServiceImpl();
    FieldUtils.writeField(parsedRecordService, "dao", parsedRecordDao, true);
    FieldUtils.writeField(parsedRecordService, "recordDao", recordDao, true);
  }

  @Override
  public void createDependentEntities(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    snapshotDao.save(TestMocks.getSnapshots(), TENANT_ID).onComplete(saveSnapshots -> {
      if (saveSnapshots.failed()) {
        context.fail(saveSnapshots.cause());
      }
      recordDao.save(TestMocks.getRecords(), TENANT_ID).onComplete(saveRecords -> {
        if (saveRecords.failed()) {
          context.fail(saveRecords.cause());
        }
        parsedRecordDao.save(TestMocks.getParsedRecords(), TENANT_ID).onComplete(saveParsedRecords -> {
          if (saveParsedRecords.failed()) {
            context.fail(saveParsedRecords.cause());
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
    String sql = format(DELETE_SQL_TEMPLATE, parsedRecordDao.getTableName());
    pgClient.execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      String recordSql = format(DELETE_SQL_TEMPLATE, recordDao.getTableName());
      pgClient.execute(recordSql, recordDelete -> {
        if (recordDelete.failed()) {
          context.fail(recordDelete.cause());
        }
        String snapshotSql = format(DELETE_SQL_TEMPLATE, snapshotDao.getTableName());
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
  public void shouldUpdateParsedRecord(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    ParsedRecord parsedRecord = record.getParsedRecord();
    parsedRecordService.updateParsedRecord(record, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateParsedRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    List<ParsedRecord> parsedRecords = records.stream()
          .map(record -> record.getParsedRecord())
          .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
    parsedRecordService.updateParsedRecords(recordCollection, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      context.assertEquals(parsedRecords.size(), update.result().getTotalRecords());
      context.assertEquals(0, update.result().getErrorMessages().size());
      async.complete();
    });
  }

}