package org.folio.dao;

import static org.folio.dao.RecordDaoImpl.INDEXERS_DELETION_LOCK_NAMESPACE_ID;
import static org.folio.rest.jaxrs.model.Record.State.ACTUAL;
import static org.folio.rest.jaxrs.model.Record.State.DELETED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.util.AdvisoryLockUtil;
import org.folio.dao.util.IdType;
import org.folio.dao.util.MatchField;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.entities.RecordsModifierOperator;
import org.folio.services.util.TypeConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RecordDaoImplTest extends AbstractLBServiceTest {

  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;
  private Record marcRecord;
  private Record deletedRecord;
  private String deletedRecordId;
  private Map<String, String> okapiHeaders;
  private RawRecord rawRecord;
  private ParsedRecord parsedRecord;

  @BeforeEach
  void setUp(VertxTestContext testContext) throws IOException {
    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord()
      .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));

    Snapshot snapshot = TestMocks.getSnapshot(0);
    String recordId = UUID.randomUUID().toString();
    deletedRecordId = UUID.randomUUID().toString();

    this.marcRecord = new Record()
      .withId(recordId)
      .withState(ACTUAL)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord.withId(recordId))
      .withParsedRecord(parsedRecord.withId(recordId))
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
      .withParsedRecord(parsedRecord.withId(recordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));

    okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(marcRecord, okapiHeaders))
      .compose(savedSnapshot -> recordDao.saveRecord(deletedRecord, okapiHeaders))
      .onComplete(testContext.succeedingThenComplete());
  }

  @AfterEach
  void cleanUp(VertxTestContext testContext) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldReturnMultipleRecordsOnGetMatchedRecordsIfMatchedRecordIdsNotSpecified(VertxTestContext testContext) {
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
      .withParsedRecord(parsedRecord.withId(copyRecordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));

    Future<List<Record>> future = SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), copyRecordSnapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(copyRecord, okapiHeaders))
      .compose(v -> recordDao.getMatchedRecords(matchField, null, TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID));

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(2, result.size());
      List<String> ids = result.stream().map(Record::getId).toList();
      assertTrue(ids.contains(copyRecord.getId()));
      assertTrue(ids.contains(marcRecord.getId()));
      recordDao.deleteRecordsBySnapshotId(copyRecordSnapshot.getJobExecutionId(), TENANT_ID)
        .onComplete(v -> testContext.completeNow());
    })));
  }

  @Test
  void shouldReturnSingleRecordsOnGetMatchedRecordsIfMatchedRecordIdsSpecified(VertxTestContext testContext) {
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
      .withParsedRecord(parsedRecord.withId(copyRecordId))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString()));

    Future<List<Record>> future = SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), copyRecordSnapshot)
      .compose(savedSnapshot -> recordDao.saveRecord(copyRecord, okapiHeaders))
      .compose(v -> recordDao.getMatchedRecords(matchField, List.of(marcRecord.getId(), UUID.randomUUID().toString(), UUID.randomUUID().toString()), TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID));

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(1, result.size());
      assertEquals(marcRecord.getId(), result.getFirst().getId());
      recordDao.deleteRecordsBySnapshotId(copyRecordSnapshot.getJobExecutionId(), TENANT_ID)
        .onComplete(v -> testContext.completeNow());
    })));
  }

  @Test
  void shouldReturnDeletedRecord(VertxTestContext testContext) {
    Future<Optional<Record>> future = recordDao.getRecordByExternalId(deletedRecordId, IdType.RECORD, TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertTrue(result.isPresent());
      assertEquals(deletedRecord.getId(), result.get().getId());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnEmptyListIfValueFieldIsEmpty(VertxTestContext testContext) {
    var matchField = new MatchField("010", "1", "", "a", MissingValue.getInstance());

    var future = recordDao.getMatchedRecords(matchField, null, TypeConnection.MARC_BIB, true, 0, 10, TENANT_ID);

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(0, result.size());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnFalseWhenPreviousIndexersDeletionIsInProgress(VertxTestContext testContext) {
    Future<Boolean> future = postgresClientFactory.getQueryExecutor(TENANT_ID)
    // gets lock on DB in same way as deleteMarcIndexersOldVersions() method to model indexers deletion being in progress
      .transaction(queryExecutor -> AdvisoryLockUtil.acquireLock(queryExecutor, INDEXERS_DELETION_LOCK_NAMESPACE_ID, TENANT_ID.hashCode())
        .compose(v -> recordDao.deleteMarcIndexersOldVersions(TENANT_ID, 2)));

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertFalse(result);
      testContext.completeNow();
    })));
  }


  @Test
  void shouldPublishRecordUpdatedWithPreModificationSnapshotWhenSavingByExternalIds(VertxTestContext testContext) {
    // Build a self-contained fixture on a COMMITTED snapshot so that persistDatabaseRecords()
    // will mark the previous version as OLD (its lookup filters by committed snapshots only)
    // and the re-fetch inside the DAO returns strictly the freshly saved version.
    Snapshot committedSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    String seedRecordId = UUID.randomUUID().toString();
    String seedExternalId = UUID.randomUUID().toString();
    String originalContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH);
    // Replace a controlled field value inside the MARC JSON so that the content stays
    // structurally valid and formatRecord() does not fall back to an ErrorRecord.
    String originalMarker = "20141107001016.0";
    String modifiedMarker = "20990101000000.0";
    assertTrue(originalContent.contains(originalMarker));
    String modifiedContent = originalContent.replace(originalMarker, modifiedMarker);
    Record seedRecord = new Record()
      .withId(seedRecordId)
      .withState(ACTUAL)
      .withMatchedId(seedRecordId)
      .withSnapshotId(committedSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(new RawRecord().withId(seedRecordId).withContent(rawRecord.getContent()))
      .withParsedRecord(new ParsedRecord().withId(seedRecordId).withContent(originalContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(seedExternalId));

    Snapshot updateSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    RecordsModifierOperator modifier = collection -> {
      collection.getRecords().forEach(r -> {
        String newRecordId = UUID.randomUUID().toString();
        r.setId(newRecordId);
        r.setSnapshotId(updateSnapshot.getJobExecutionId());
        r.getRawRecord().setId(newRecordId);
        r.getParsedRecord().setId(newRecordId);
        r.getParsedRecord().setContent(modifiedContent);
      });
      return collection;
    };

    Future<RecordsBatchResponse> future = SnapshotDaoUtil
      .save(postgresClientFactory.getQueryExecutor(TENANT_ID), committedSnapshot)
      .compose(v -> recordDao.saveRecord(seedRecord, okapiHeaders))
      .compose(v -> {
        // Fixture setUp and the seed record create three records via saveRecord() which trigger
        // publishRecordCreated(); reset the counters so the verifications below only observe
        // the interactions caused by the method under test.
        clearInvocations(recordDomainEventPublisher);
        return SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), updateSnapshot);
      })
      .compose(v -> recordDao.saveRecordsByExternalIds(List.of(seedExternalId), RecordType.MARC_BIB, modifier, okapiHeaders));

    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(1, result.getTotalRecords());

      ArgumentCaptor<Record> oldCaptor = ArgumentCaptor.forClass(Record.class);
      ArgumentCaptor<Record> newCaptor = ArgumentCaptor.forClass(Record.class);
      verify(recordDomainEventPublisher, times(1))
        .publishRecordUpdated(oldCaptor.capture(), newCaptor.capture(), eq(okapiHeaders));
      verify(recordDomainEventPublisher, never()).publishRecordCreated(any(), any());

      Record capturedOld = oldCaptor.getValue();
      Record capturedNew = newCaptor.getValue();
      // Deep-clone contract: the "old" record must carry the pre-modification content
      // even though the modifier has already mutated the shared record instance in-place.
      String oldContent = capturedOld.getParsedRecord().getContent().toString();
      String newContent = capturedNew.getParsedRecord().getContent().toString();
      assertTrue(oldContent.contains(originalMarker));
      assertFalse(oldContent.contains(modifiedMarker));
      assertTrue(newContent.contains(modifiedMarker));
      assertFalse(newContent.contains(originalMarker));
      assertEquals(seedRecordId, capturedOld.getId());
      assertNotEquals(capturedOld.getId(), capturedNew.getId());
      assertEquals(seedRecord.getMatchedId(), capturedOld.getMatchedId());
      assertEquals(seedRecord.getMatchedId(), capturedNew.getMatchedId());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldReturnEmptyResponseAndNotPublishEventsWhenNoRecordsFoundByExternalIds(VertxTestContext testContext) {
    clearInvocations(recordDomainEventPublisher);
    String unknownExternalId = UUID.randomUUID().toString();
    RecordsModifierOperator modifier = collection -> collection;

    vertx.runOnContext(v -> {
      Future<RecordsBatchResponse> future = recordDao.saveRecordsByExternalIds(
        List.of(unknownExternalId), RecordType.MARC_BIB, modifier, okapiHeaders);

      future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertEquals(0, result.getTotalRecords());
        verify(recordDomainEventPublisher, never()).publishRecordUpdated(any(), any(), any());
        verify(recordDomainEventPublisher, never()).publishRecordCreated(any(), any());
        testContext.completeNow();
      })));
    });
  }

  @Test
  void shouldSkipDeletedRecordsWhenSavingByExternalIds(VertxTestContext testContext) {
    clearInvocations(recordDomainEventPublisher);
    RecordsModifierOperator modifier = collection -> collection;

    String deletedExternalId = deletedRecord.getExternalIdsHolder().getInstanceId();
    vertx.runOnContext(v -> {
      Future<RecordsBatchResponse> future = recordDao.saveRecordsByExternalIds(
        List.of(deletedExternalId), RecordType.MARC_BIB, modifier, okapiHeaders);

      future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertEquals(0, result.getTotalRecords());
        verify(recordDomainEventPublisher, never()).publishRecordUpdated(any(), any(), any());
        verify(recordDomainEventPublisher, never()).publishRecordCreated(any(), any());
        testContext.completeNow();
      })));
    });
  }
}
