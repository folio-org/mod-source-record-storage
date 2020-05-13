package org.folio.dao.impl;

import static org.folio.SourceRecordTestHelper.compareSourceRecord;
import static org.folio.SourceRecordTestHelper.compareSourceRecordCollection;
import static org.folio.SourceRecordTestHelper.compareSourceRecords;
import static org.folio.SourceRecordTestHelper.getParsedRecords;
import static org.folio.SourceRecordTestHelper.getRecords;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.folio.TestMocks;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.SourceRecordContent;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.persist.PostgresClient;
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
public class SourceRecordDaoTest extends AbstractDaoTest {

  private LBSnapshotDao snapshotDao;
  private LBRecordDao recordDao;
  private RawRecordDao rawRecordDao;
  private ParsedRecordDao parsedRecordDao;
  private ErrorRecordDao errorRecordDao;
  private SourceRecordDao sourceRecordDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
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
    sourceRecordDao = new SourceRecordDaoImpl();
    FieldUtils.writeField(sourceRecordDao, "postgresClientFactory", postgresClientFactory, true);
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
        CompositeFuture.all(
          rawRecordDao.save(TestMocks.getRawRecords(), TENANT_ID),
          parsedRecordDao.save(TestMocks.getParsedRecords(), TENANT_ID),
          errorRecordDao.save(TestMocks.getErrorRecords(), TENANT_ID)
        ).onComplete(save -> {
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
    ).onComplete(delete -> {
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
    sourceRecordDao.getSourceMarcRecordById(TestMocks.getRecord(0).getId(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, TestMocks.getRecord(0), TestMocks.getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByIdAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordByIdAlt(TestMocks.getRecord(0).getId(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected record and parsed record should be updated
      compareSourceRecord(context, TestMocks.getRecord(0), TestMocks.getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceId(TestContext context) {
    Async async = context.async();
    String instanceId = TestMocks.getRecord(0).getExternalIdsHolder().getInstanceId();
    sourceRecordDao.getSourceMarcRecordByInstanceId(instanceId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, TestMocks.getRecord(0), TestMocks.getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceIdAlt(TestContext context) {
    Async async = context.async();
    String instanceId = TestMocks.getRecord(0).getExternalIdsHolder().getInstanceId();
    sourceRecordDao.getSourceMarcRecordByInstanceIdAlt(instanceId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected record and parsed record should be updated
      compareSourceRecord(context, TestMocks.getRecord(0), TestMocks.getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecords(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecords(0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<RawRecord> expectedRawRecords = new ArrayList<>();
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsEmptyResults(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecords(10, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      context.assertEquals(new Integer(expectedRecords.size()), res.result().getTotalRecords());
      context.assertTrue(res.result().getSourceRecords().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsAlt(0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected records and parsed records will have to be manually filtered
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<RawRecord> expectedRawRecords = new ArrayList<>();
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsAltEmptyResults(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsAlt(0, 0, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected records and parsed records will have to be manually filtered
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      context.assertEquals(new Integer(expectedRecords.size()), res.result().getTotalRecords());
      context.assertTrue(res.result().getSourceRecords().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriod(TestContext context) throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    DateUtils.addHours(new Date(), 1);
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsForPeriod(from, till, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<RawRecord> expectedRawRecords = new ArrayList<>();
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriodEmptyResults(TestContext context) throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    DateUtils.addHours(new Date(), 1);
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsForPeriod(from, till, 0, 0, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      context.assertEquals(new Integer(expectedRecords.size()), res.result().getTotalRecords());
      context.assertTrue(res.result().getSourceRecords().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriodAlt(TestContext context) throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsForPeriodAlt(from, till, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected records and parsed records will have to be manually filtered
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<RawRecord> expectedRawRecords = new ArrayList<>();
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriodAltEmptyResults(TestContext context) throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsForPeriodAlt(from, till, 10, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected records and parsed records will have to be manually filtered
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      context.assertEquals(new Integer(expectedRecords.size()), res.result().getTotalRecords());
      context.assertTrue(res.result().getSourceRecords().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldStreamGetSourceMarcRecordsByQuery(TestContext context) {
    Async async = context.async();
    SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;
    RecordQuery query = new RecordQuery();
    List<SourceRecord> actualSourceRecords = new ArrayList<>();
    sourceRecordDao.getSourceMarcRecordsByQuery(content, query, 0, 10, TENANT_ID, stream -> {
      stream.handler(row -> actualSourceRecords.add(sourceRecordDao.toSourceRecord(row)));
    }, finished -> {
      if (finished.failed()) {
        context.fail(finished.cause());
      }
      List<Record> expectedRecords = TestMocks.getRecords();
      List<RawRecord> expectedRawRecords = new ArrayList<>();
      List<ParsedRecord> expectedParsedRecords = new ArrayList<>();
      compareSourceRecords(context, expectedRecords, expectedRawRecords, expectedParsedRecords, actualSourceRecords);
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