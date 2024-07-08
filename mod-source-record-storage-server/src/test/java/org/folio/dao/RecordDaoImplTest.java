package org.folio.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.reactivex.FlowableHelper;
import io.vertx.sqlclient.Row;
import net.sf.jsqlparser.JSQLParserException;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.util.AdvisoryLockUtil;
import org.folio.dao.util.IdType;
import org.folio.dao.util.MatchField;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.RecordSearchParameters;
import org.folio.services.util.TypeConnection;
import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.folio.services.util.parser.SearchExpressionParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.util.ReflectionTestUtils;

import javax.ws.rs.DELETE;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.dao.RecordDaoImpl.INDEXERS_DELETION_LOCK_NAMESPACE_ID;
import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.jaxrs.model.Record.State.DELETED;
import static org.folio.rest.jaxrs.model.Record.State.OLD;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_TRACKING;
import static org.folio.rest.jooq.Tables.RECORDS_LB;

@RunWith(VertxUnitRunner.class)
public class RecordDaoImplTest extends AbstractLBServiceTest {

  private static final String ENABLE_FALLBACK_QUERY_FIELD = "enableFallbackQuery";

  private RecordDao recordDao;
  private Record record;
  private Record deletedRecord;
  private String deletedRecordId;

  @Before
  public void setUp(TestContext context) throws IOException {
    Async async = context.async();
    recordDao = new RecordDaoImpl(postgresClientFactory);
    RawRecord rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    ParsedRecord marcRecord = new ParsedRecord()
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Snapshot snapshot = TestMocks.getSnapshot(0);
    String recordId = UUID.randomUUID().toString();
    deletedRecordId = UUID.randomUUID().toString();

    this.record = new Record()
      .withId(recordId)
      .withState(ACTUAL)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord.withId(recordId))
      .withParsedRecord(marcRecord.withId(recordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));


    this.deletedRecord = new Record()
      .withId(deletedRecordId)
      .withState(DELETED)
      .withMatchedId(deletedRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord.withId(recordId))
      .withParsedRecord(marcRecord.withId(recordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(record, TENANT_ID))
      .compose(savedSnapshot -> recordDao.saveRecord(deletedRecord, TENANT_ID))
      .onComplete(save -> {
        if (save.failed()) {
          context.fail(save.cause());
        }
        async.complete();
      });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldReturnRecordOnGetMatchedRecordsWhenThereIsNoTrackingRecordAndFallbackQueryEnabled(TestContext context) {
    Async async = context.async();
    ReflectionTestUtils.setField(recordDao, ENABLE_FALLBACK_QUERY_FIELD, true);
    MatchField matchField = new MatchField("100", "1", "", "a", StringValue.of("Mozart, Wolfgang Amadeus,"));

    Future<List<Record>> future = deleteTrackingRecordById(record.getId())
      .compose(v -> recordDao.getMatchedRecords(matchField, TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(1, ar.result().size());
      context.assertEquals(record.getId(), ar.result().get(0).getId());
      async.complete();
    });
  }

  @Test
  public void shouldReturnDeletedRecord(TestContext context) {
    Async async = context.async();

    Future<Optional<Record>> future =  recordDao.getRecordByExternalId(deletedRecordId, IdType.RECORD, TENANT_ID);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      context.assertEquals(deletedRecord.getId(), ar.result().get().getId());
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyListIfValueFieldIsEmpty(TestContext context) {
    var async = context.async();
    var matchField = new MatchField("010", "1", "", "a", MissingValue.getInstance());

    var future = recordDao.getMatchedRecords(matchField, TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(0, ar.result().size());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFalseWhenPreviousIndexersDeletionIsInProgress(TestContext context) {
    Async async = context.async();

    Future<Boolean> future = postgresClientFactory.getQueryExecutor(TENANT_ID)
    // gets lock on DB in same way as deleteMarcIndexersOldVersions() method to model indexers deletion being in progress
      .transaction(txQE -> AdvisoryLockUtil.acquireLock(txQE, INDEXERS_DELETION_LOCK_NAMESPACE_ID, TENANT_ID.hashCode())
        .compose(v -> recordDao.deleteMarcIndexersOldVersions(TENANT_ID)));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertFalse(ar.result());
      async.complete();
    });
  }

  private Future<Boolean> deleteTrackingRecordById(String recordId) {
    return postgresClientFactory.getQueryExecutor(TENANT_ID).execute(dslContext -> dslContext
        .deleteFrom(MARC_RECORDS_TRACKING)
        .where(MARC_RECORDS_TRACKING.MARC_ID.eq(UUID.fromString(recordId))))
      .map(deleted -> deleted != 0);
  }

}
