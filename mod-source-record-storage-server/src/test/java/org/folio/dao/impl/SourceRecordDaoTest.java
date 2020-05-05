package org.folio.dao.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SourceRecordDaoTest extends AbstractDaoTest {

  private SourceRecordDao sourceRecordDao;

  private LBSnapshotDao snapshotDao;
  private LBRecordDao recordDao;
  private RawRecordDao rawRecordDao;
  private ParsedRecordDao parsedRecordDao;
  private ErrorRecordDao errorRecordDao;

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    sourceRecordDao = new SourceRecordDaoImpl();
    FieldUtils.writeField(sourceRecordDao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentBeans(TestContext context) throws IllegalAccessException {
    Async async = context.async();
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
    snapshotDao.save(getSnapshots(), TENANT_ID).setHandler(saveSnapshots -> {
      if (saveSnapshots.failed()) {
        context.fail(saveSnapshots.cause());
      }
      recordDao.save(getRecords(), TENANT_ID).setHandler(saveRecords -> {
        if (saveRecords.failed()) {
          context.fail(saveRecords.cause());
        }
        CompositeFuture.all(
          rawRecordDao.save(getRawRecords(), TENANT_ID),
          parsedRecordDao.save(getParsedRecords(), TENANT_ID),
          errorRecordDao.save(getErrorRecords(), TENANT_ID)
        ).setHandler(save -> {
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
    ).setHandler(delete -> {
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
    sourceRecordDao.getSourceMarcRecordById(getRecord(0).getId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, getRecord(0), getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByIdAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordByIdAlt(getRecord(0).getId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected record and parsed record should be updated
      compareSourceRecord(context, getRecord(0), getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceId(TestContext context) {
    Async async = context.async();
    String instanceId = getRecord(0).getExternalIdsHolder().getInstanceId();
    sourceRecordDao.getSourceMarcRecordByInstanceId(instanceId, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, getRecord(0), getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceIdAlt(TestContext context) {
    Async async = context.async();
    String instanceId = getRecord(0).getExternalIdsHolder().getInstanceId();
    sourceRecordDao.getSourceMarcRecordByInstanceIdAlt(instanceId, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected record and parsed record should be updated
      compareSourceRecord(context, getRecord(0), getParsedRecord(0), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecords(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecords(0, 10, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedParsedRecords, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsAlt(TestContext context) {
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsAlt(0, 10, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected records and parsed records will have to be manually filtered
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedParsedRecords, res.result());
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
    sourceRecordDao.getSourceMarcRecordsForPeriod(from, till, 0, 10, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedParsedRecords, res.result());
      async.complete();
    });
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriodAlt(TestContext context) throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    Async async = context.async();
    sourceRecordDao.getSourceMarcRecordsForPeriodAlt(from, till, 0, 10, TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected records and parsed records will have to be manually filtered
      List<Record> expectedRecords = getRecords(State.ACTUAL);
      List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
      compareSourceRecordCollection(context, expectedRecords, expectedParsedRecords, res.result());
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

  private void compareSourceRecord(
    TestContext context,
    Record expectedRecord,
    ParsedRecord expectedParsedRecord,
    Optional<SourceRecord> actualSourceRecord
  ) {
    context.assertTrue(actualSourceRecord.isPresent());
    context.assertEquals(expectedRecord.getId(), actualSourceRecord.get().getRecordId());
    context.assertEquals(new JsonObject((String) expectedParsedRecord.getContent()),
      new JsonObject((String) actualSourceRecord.get().getParsedRecord().getContent()));
    context.assertEquals(expectedParsedRecord.getFormattedContent().trim(),
      actualSourceRecord.get().getParsedRecord().getFormattedContent().trim());
  }

  private void compareSourceRecordCollection(
    TestContext context, 
    List<Record> expectedRecords, 
    List<ParsedRecord> expectedParsedRecords, 
    SourceRecordCollection actualSourceRecordCollection
  ) {
    Collections.sort(expectedRecords, (r1, r2) -> r1.getId().compareTo(r2.getId()));

    Collections.sort(expectedParsedRecords, (pr1, pr2) -> pr1.getId().compareTo(pr2.getId()));

    List<SourceRecord> actualSourceRecords = actualSourceRecordCollection.getSourceRecords();

    Collections.sort(actualSourceRecords, (sr1, sr2) -> sr1.getRecordId().compareTo(sr2.getRecordId()));

    context.assertEquals(expectedRecords.size(), actualSourceRecordCollection.getTotalRecords());
    context.assertEquals(expectedParsedRecords.size(), actualSourceRecordCollection.getTotalRecords());

    for (int i = 0; i < expectedRecords.size(); i++) {
      Record expectedRecord = expectedRecords.get(i);
      ParsedRecord expectedParsedRecord = expectedParsedRecords.get(i);
      SourceRecord actualSourceRecord = actualSourceRecords.get(i);
      context.assertEquals(expectedRecord.getId(), actualSourceRecord.getRecordId());
      context.assertEquals(new JsonObject((String) expectedParsedRecord.getContent()).encode(),
        new JsonObject((String) actualSourceRecord.getParsedRecord().getContent()).encode());
      context.assertEquals(expectedParsedRecord.getFormattedContent().trim(),
        actualSourceRecord.getParsedRecord().getFormattedContent().trim());
    }
  }

  private List<Record> getRecords(State state) {
    return getRecords().stream()
      .filter(expectedRecord -> expectedRecord.getState().equals(state)).collect(Collectors.toList());
  }

  private List<ParsedRecord> getParsedRecords(List<Record> records) {
    return records.stream()
      .map(record -> getParsedRecord(record.getId()))
      .filter(parsedRecord -> parsedRecord.isPresent()).map(parsedRecord -> parsedRecord.get())
      .collect(Collectors.toList());
  }

}