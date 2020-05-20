package org.folio.services.impl;

import static java.lang.String.format;

import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.TestMocks;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.impl.AbstractDaoTest;
import org.folio.dao.impl.ErrorRecordDaoImpl;
import org.folio.dao.impl.LBRecordDaoImpl;
import org.folio.dao.impl.LBSnapshotDaoImpl;
import org.folio.dao.impl.ParsedRecordDaoImpl;
import org.folio.dao.impl.RawRecordDaoImpl;
import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.LBRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceDaoTest extends AbstractDaoTest {

  LBRecordService recordService;

  LBSnapshotDao snapshotDao;

  LBRecordDao recordDao;

  RawRecordDao rawRecordDao;

  ParsedRecordDao parsedRecordDao;

  ErrorRecordDao errorRecordDao;

  @Override
  public void createBeans(TestContext context) throws IllegalAccessException {
    snapshotDao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(snapshotDao, "postgresClientFactory", postgresClientFactory, true);
    recordDao = new LBRecordDaoImpl();
    FieldUtils.writeField(recordDao, "postgresClientFactory", postgresClientFactory, true);
    rawRecordDao = new RawRecordDaoImpl();
    FieldUtils.writeField(rawRecordDao, "postgresClientFactory", postgresClientFactory, true);
    parsedRecordDao = new ParsedRecordDaoImpl();
    FieldUtils.writeField(parsedRecordDao, "postgresClientFactory", postgresClientFactory, true);
    errorRecordDao = new ErrorRecordDaoImpl();
    FieldUtils.writeField(errorRecordDao, "postgresClientFactory", postgresClientFactory, true);

    recordService = new LBRecordServiceImpl();
    FieldUtils.writeField(recordService, "dao", recordDao, true);
    FieldUtils.writeField(recordService, "snapshotDao", snapshotDao, true);
    FieldUtils.writeField(recordService, "rawRecordDao", rawRecordDao, true);
    FieldUtils.writeField(recordService, "parsedRecordDao", parsedRecordDao, true);
    FieldUtils.writeField(recordService, "errorRecordDao", errorRecordDao, true);
  }

  @Override
  public void createDependentEntities(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    snapshotDao.save(TestMocks.getSnapshots(), TENANT_ID).onComplete(saveSnapshots -> {
      if (saveSnapshots.failed()) {
        context.fail(saveSnapshots.cause());
      }
      async.complete();
    });
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String rawRecordSql = format(DELETE_SQL_TEMPLATE, rawRecordDao.getTableName());
    String parsedRecordSql = format(DELETE_SQL_TEMPLATE, parsedRecordDao.getTableName());
    String errorRecordSql = format(DELETE_SQL_TEMPLATE, errorRecordDao.getTableName());
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
    ).onComplete(delete -> {
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
  public void shouldSave(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    recordService.save(record, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertTrue(save.succeeded());
      async.complete();
    });
  }

  @Test
  public void shouldSaveRecords(TestContext context) {
    Async async = context.async();
    List<Record> records = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());
      recordService.saveRecords(recordCollection, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertTrue(save.succeeded());
      context.assertEquals(records.size(), save.result().getTotalRecords());
      context.assertEquals(0, save.result().getErrorMessages().size());
      async.complete();
    });
  }

  @Test
  public void shouldUpdate(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    recordDao.save(record, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertTrue(save.succeeded());
      Record updateRecord = new Record()
        .withId(record.getId())
        .withSnapshotId(record.getSnapshotId())
        .withMatchedId(TestMocks.getRecord(1).getMatchedId())
        .withMatchedProfileId(record.getMatchedProfileId())
        .withGeneration(record.getGeneration())
        .withOrder(record.getOrder())
        .withRecordType(record.getRecordType())
        .withRawRecord(record.getRawRecord())
        .withParsedRecord(record.getParsedRecord())
        .withExternalIdsHolder(record.getExternalIdsHolder())
        .withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(false))
        .withMetadata(record.getMetadata());
      recordService.update(updateRecord, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertTrue(update.succeeded());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetFormattedRecord(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    ParsedRecord parsedRecord = record.getParsedRecord();
    recordDao.save(record, TENANT_ID).onComplete(saveRecord -> {
      if (saveRecord.failed()) {
        context.fail(saveRecord.cause());
      }
      context.assertTrue(saveRecord.succeeded());
      parsedRecordDao.save(parsedRecord, TENANT_ID).onComplete(saveParsedRecord -> {
        if (saveParsedRecord.failed()) {
          context.fail(saveParsedRecord.cause());
        }
        context.assertTrue(saveParsedRecord.succeeded());
        String instanceId = record.getExternalIdsHolder().getInstanceId();
        recordService.getFormattedRecord(ExternalIdType.INSTANCE.toString(), instanceId, TENANT_ID).onComplete(fr -> {
          if (fr.failed()) {
            context.fail(fr.cause());
          }
          context.assertTrue(fr.succeeded());
          context.assertEquals(instanceId, fr.result().getExternalIdsHolder().getInstanceId());
          context.assertEquals(parsedRecord.getFormattedContent(), fr.result().getParsedRecord().getFormattedContent());
          async.complete();
        });
      });
    });
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForRecord(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    recordDao.save(record, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertTrue(save.succeeded());
      SuppressFromDiscoveryDto suppressFromDiscoveryDto = new SuppressFromDiscoveryDto()
        .withId(record.getExternalIdsHolder().getInstanceId())
        .withIncomingIdType(IncomingIdType.INSTANCE)
        .withSuppressFromDiscovery(true);
      recordService.updateSuppressFromDiscoveryForRecord(suppressFromDiscoveryDto, TENANT_ID).onComplete(update -> {
        if (update.failed()) {
          context.fail(update.cause());
        }
        context.assertTrue(update.succeeded());
        context.assertTrue(update.result().getAdditionalInfo().getSuppressDiscovery());
        async.complete();
      });
    });
  }

  @Test
  public void shouldUpdateSourceRecord(TestContext context) {
    Async async = context.async();
    Record record = TestMocks.getRecord(0);
    recordDao.save(record, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      rawRecordDao.save(record.getRawRecord(), TENANT_ID).onComplete(saveRawRecord -> {
        if (saveRawRecord.failed()) {
          context.fail(saveRawRecord.cause());
        }
        context.assertTrue(saveRawRecord.succeeded());
        ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
          .withId(record.getId())
          .withRecordType(ParsedRecordDto.RecordType.fromValue(record.getRecordType().toString()))
          .withParsedRecord(record.getParsedRecord())
          .withExternalIdsHolder(record.getExternalIdsHolder())
          .withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(true))
          .withMetadata(record.getMetadata());
        recordService.updateSourceRecord(parsedRecordDto, "a16de3f5-5751-4d8f-82a4-0b9f21238c51", TENANT_ID).onComplete(update -> {
          if (update.failed()) {
            context.fail(update.cause());
          }
          context.assertTrue(update.succeeded());
          context.assertTrue(update.result().getAdditionalInfo().getSuppressDiscovery());
          async.complete();
        });
      });
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