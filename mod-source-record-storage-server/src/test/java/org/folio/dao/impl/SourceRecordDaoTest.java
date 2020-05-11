package org.folio.dao.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

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
    List<RawRecord> expectedRawRecords,
    List<ParsedRecord> expectedParsedRecords, 
    SourceRecordCollection actualSourceRecordCollection
  ) {
    List<SourceRecord> actualSourceRecords = actualSourceRecordCollection.getSourceRecords();
    context.assertEquals(expectedRecords.size(), actualSourceRecordCollection.getTotalRecords());
    context.assertTrue(actualSourceRecordCollection.getTotalRecords() >= expectedRawRecords.size());
    context.assertTrue(actualSourceRecordCollection.getTotalRecords() >= expectedParsedRecords.size());
    compareSourceRecords(context, expectedRecords, expectedRawRecords, expectedParsedRecords, actualSourceRecords);
  }

  private void compareSourceRecords(
    TestContext context, 
    List<Record> expectedRecords,
    List<RawRecord> expectedRawRecords,
    List<ParsedRecord> expectedParsedRecords, 
    List<SourceRecord> actualSourceRecords
  ) {

    context.assertEquals(expectedRecords.size(), actualSourceRecords.size());

    Collections.sort(expectedRecords, (r1, r2) -> r1.getId().compareTo(r2.getId()));
    Collections.sort(expectedRawRecords, (rr1, rr2) -> rr1.getId().compareTo(rr2.getId()));
    Collections.sort(expectedParsedRecords, (pr1, pr2) -> pr1.getId().compareTo(pr2.getId()));
    Collections.sort(actualSourceRecords, (sr1, sr2) -> sr1.getRecordId().compareTo(sr2.getRecordId()));

    expectedRawRecords.forEach(expectedRawRecord -> {
      Optional<SourceRecord> actualSourceRecord = actualSourceRecords.stream().filter(sourceRecord -> {
        return Objects.nonNull(sourceRecord.getRawRecord())
          && sourceRecord.getRawRecord().getId().equals(expectedRawRecord.getId());
      }).findAny();
      context.assertTrue(actualSourceRecord.isPresent());
      context.assertEquals(expectedRawRecord.getContent(),
        actualSourceRecord.get().getRawRecord().getContent());
    });

    expectedParsedRecords.forEach(expectedParsedRecord -> {
      Optional<SourceRecord> actualSourceRecord = actualSourceRecords.stream().filter(sourceRecord -> {
        return Objects.nonNull(sourceRecord.getParsedRecord())
          && sourceRecord.getParsedRecord().getId().equals(expectedParsedRecord.getId());
      }).findAny();
      context.assertTrue(actualSourceRecord.isPresent());
      context.assertEquals(new JsonObject((String) expectedParsedRecord.getContent()).encode(),
        new JsonObject((String) actualSourceRecord.get().getParsedRecord().getContent()).encode());
      context.assertEquals(expectedParsedRecord.getFormattedContent().trim(),
        actualSourceRecord.get().getParsedRecord().getFormattedContent().trim());
    });

    for (int i = 0; i < expectedRecords.size(); i++) {
      Record expectedRecord = expectedRecords.get(i);
      SourceRecord actualSourceRecord = actualSourceRecords.get(i);
      context.assertEquals(expectedRecord.getId(), actualSourceRecord.getRecordId());
      if (Objects.nonNull(actualSourceRecord.getSnapshotId())) {
        context.assertEquals(expectedRecord.getSnapshotId(), actualSourceRecord.getSnapshotId());
      }
      if (Objects.nonNull(actualSourceRecord.getOrder())) {
        context.assertEquals(expectedRecord.getOrder(), actualSourceRecord.getOrder());
      }
      if (Objects.nonNull(actualSourceRecord.getRecordType())) {
        context.assertEquals(expectedRecord.getRecordType().toString(),
          actualSourceRecord.getRecordType().toString());
      }
      if (Objects.nonNull(actualSourceRecord.getExternalIdsHolder())) {
        context.assertEquals(expectedRecord.getExternalIdsHolder().getInstanceId(),
          actualSourceRecord.getExternalIdsHolder().getInstanceId());
      }
      if (Objects.nonNull(actualSourceRecord.getMetadata())) {
        context.assertEquals(expectedRecord.getMetadata().getCreatedByUserId(),
          actualSourceRecord.getMetadata().getCreatedByUserId());
        context.assertNotNull(actualSourceRecord.getMetadata().getCreatedDate());
        context.assertEquals(expectedRecord.getMetadata().getUpdatedByUserId(),
          actualSourceRecord.getMetadata().getUpdatedByUserId());
        context.assertNotNull(actualSourceRecord.getMetadata().getUpdatedDate());
      }
    }
  }

  private List<Record> getRecords(State state) {
    return TestMocks.getRecords().stream()
      .filter(expectedRecord -> expectedRecord.getState().equals(state))
      .collect(Collectors.toList());
  }

  private List<ParsedRecord> getParsedRecords(List<Record> records) {
    return records.stream()
      .map(record -> TestMocks.getParsedRecord(record.getId()))
      .filter(parsedRecord -> parsedRecord.isPresent()).map(parsedRecord -> parsedRecord.get())
      .collect(Collectors.toList());
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