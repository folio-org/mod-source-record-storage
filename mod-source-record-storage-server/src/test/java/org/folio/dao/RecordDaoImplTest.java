package org.folio.dao;

import static org.folio.dao.RecordDaoImpl.INDEXERS_DELETION_LOCK_NAMESPACE_ID;
import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.jaxrs.model.Record.State.DELETED;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.util.AdvisoryLockUtil;
import org.folio.dao.util.IdType;
import org.folio.dao.util.MatchField;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.util.TypeConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class RecordDaoImplTest extends AbstractLBServiceTest {

  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;
  private Record record;
  private Record deletedRecord;
  private String deletedRecordId;
  private Map<String, String> okapiHeaders;
  private RawRecord rawRecord;
  private ParsedRecord marcRecord;

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.openMocks(this);
    Async async = context.async();
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    marcRecord = new ParsedRecord()
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

    okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(record, okapiHeaders))
      .compose(savedSnapshot -> recordDao.saveRecord(deletedRecord, okapiHeaders))
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
  public void shouldReturnMultipleRecordsOnGetMatchedRecordsIfMatchedRecordIdsNotSpecified(TestContext context) {
    Async async = context.async();

    MatchField matchField = new MatchField("100", "1", "", "a", StringValue.of("Mozart, Wolfgang Amadeus,"));

    Snapshot copyRecordSnapshot = TestMocks.getSnapshot(1);
    String copyRecordId = UUID.randomUUID().toString();
    Record copyRecord = new Record()
      .withId(copyRecordId)
      .withState(ACTUAL)
      .withMatchedId(copyRecordId)
      .withSnapshotId(copyRecordSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord.withId(copyRecordId))
      .withParsedRecord(marcRecord.withId(copyRecordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));

    Future<List<Record>> future = SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), copyRecordSnapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(copyRecord, okapiHeaders))
      .compose(v -> recordDao.getMatchedRecords(matchField, null, null, TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(2, ar.result().size());
      List<String> ids = ar.result().stream().map(Record::getId).toList();
      context.assertTrue(ids.contains(copyRecord.getId()));
      context.assertTrue(ids.contains(record.getId()));
      recordDao.deleteRecordsBySnapshotId(copyRecordSnapshot.getJobExecutionId(), TENANT_ID)
        .onComplete(v -> async.complete());
    });
  }

  @Test
  public void shouldReturnSingleRecordsOnGetMatchedRecordsIfMatchedRecordIdsSpecified(TestContext context) {
    Async async = context.async();
    MatchField matchField = new MatchField("100", "1", "", "a", StringValue.of("Mozart, Wolfgang Amadeus,"));

    Snapshot copyRecordSnapshot = TestMocks.getSnapshot(1);
    String copyRecordId = UUID.randomUUID().toString();
    Record copyRecord = new Record()
      .withId(copyRecordId)
      .withState(ACTUAL)
      .withMatchedId(copyRecordId)
      .withSnapshotId(copyRecordSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord.withId(copyRecordId))
      .withParsedRecord(marcRecord.withId(copyRecordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));

    Future<List<Record>> future = SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), copyRecordSnapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(copyRecord, okapiHeaders))
      .compose(v -> recordDao.getMatchedRecords(matchField, null, List.of(record.getId(), UUID.randomUUID().toString(), UUID.randomUUID().toString()), TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(1, ar.result().size());
      context.assertEquals(record.getId(), ar.result().getFirst().getId());
      recordDao.deleteRecordsBySnapshotId(copyRecordSnapshot.getJobExecutionId(), TENANT_ID)
        .onComplete(v -> async.complete());
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

    var future = recordDao.getMatchedRecords(matchField, null, null, TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID);

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
      .transaction(queryExecutor -> AdvisoryLockUtil.acquireLock(queryExecutor, INDEXERS_DELETION_LOCK_NAMESPACE_ID, TENANT_ID.hashCode())
        .compose(v -> recordDao.deleteMarcIndexersOldVersions(TENANT_ID, 2)));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertFalse(ar.result());
      async.complete();
    });
  }

}
