package org.folio.dao;

import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.max;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.LBErrorRecordDaoUtil;
import org.folio.dao.util.LBParsedRecordDaoUtil;
import org.folio.dao.util.LBRawRecordDaoUtil;
import org.folio.dao.util.LBRecordDaoUtil;
import org.folio.dao.util.LBSnapshotDaoUtil;
import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.enums.RecordState;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Component
@ConditionalOnProperty(prefix = "jooq", name = "dao.record", havingValue = "true")
public class LBRecordDaoImpl implements LBRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(LBRecordDaoImpl.class);

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public LBRecordDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    // TODO: fix using effecient sql functions
    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDaoUtil.streamByCondition(txQE, condition, orderFields, offset, limit)
      .compose(stream -> {
        List<Record> records = new ArrayList<>();
        Promise<List<Record>> promise = Promise.promise();
        CompositeFuture.all(stream
          .map(pr -> lookupAssociatedRecords(txQE, pr, true)
          .map(r -> addToList(records, r)))
          .collect(Collectors.toList()))
            .onComplete(res -> promise.complete(records));
        return promise.future();
    })).map(records -> new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size()));
  }

  @Override
  public Future<Optional<Record>> getRecordById(String matchedId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, matchedId));
  }

  @Override
  public Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String matchedId) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(matchedId))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    return getRecordByCondition(txQE, condition);
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByCondition(txQE, condition));
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition) {
    return LBRecordDaoUtil.findByCondition(txQE, condition)
      .compose(record -> lookupAssociatedRecords(txQE, record, true));
  }

  @Override
  public Future<Record> saveRecord(Record record, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> LBSnapshotDaoUtil.findById(txQE, record.getSnapshotId())
      .map(optionalSnapshot -> optionalSnapshot
        .orElseThrow(() -> new NotFoundException("Couldn't find snapshot with id " + record.getSnapshotId())))
      .compose(snapshot -> {
        if (Objects.isNull(snapshot.getProcessingStartedDate())) {
          String msgTemplate = "Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s";
          String message = String.format(msgTemplate, snapshot.getStatus());
          return Future.failedFuture(new BadRequestException(message));
        }
        return Future.succeededFuture();
      }).compose(v -> {
        if (Objects.isNull(record.getGeneration())) {
          return calculateGeneration(txQE, record);
        }
        return Future.succeededFuture(record.getGeneration());
      }).compose(generation -> insertOrUpdateRecord(txQE, record.withGeneration(generation))));
  }

  @Override
  public Future<Record> updateRecord(Record record, String tenantId) {
    return getRecordById(record.getId(), tenantId)
      .compose(optionalRecord -> optionalRecord
        .map(r -> saveRecord(record, tenantId))
        .orElse(Future.failedFuture(new NotFoundException(String.format("Record with id '%s' was not found", record.getId())))));
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit,
      boolean deletedRecords, String tenantId) {
    // TODO: fix using effecient sql functions
    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDaoUtil.streamByCondition(txQE, condition, orderFields, offset, limit)
      .compose(stream -> {
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Promise<List<SourceRecord>> promise = Promise.promise();
        CompositeFuture.all(stream
          .map(pr -> lookupAssociatedRecords(txQE, pr, false)
          .map(LBRecordDaoUtil::toSourceRecord)
          .map(sr -> addToList(sourceRecords, sr)))
          .collect(Collectors.toList()))
            .onComplete(res -> promise.complete(sourceRecords));
        return promise.future();
    })).map(records -> new SourceRecordCollection()
      .withSourceRecords(records)
      .withTotalRecords(records.size()));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
        .where(condition))
          .map(LBRecordDaoUtil::toRecord)
      .compose(record -> lookupAssociatedRecords(txQE, record, false)))
        .map(LBRecordDaoUtil::toSourceRecord)
        .map(record -> {
          if (Objects.nonNull(record.getParsedRecord())) {
            return Optional.of(record);
          }
          return Optional.empty();
        });
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, String tenantId) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(id))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    return getSourceRecordByCondition(condition, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByExternalId(String externalId, ExternalIdType externalIdType, String tenantId) {
    Condition condition = LBRecordDaoUtil.getExternalIdCondition(externalId, externalIdType)
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    return getSourceRecordByCondition(condition, tenantId);
  }

  @Override
  public Future<Integer> calculateGeneration(Record record, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> calculateGeneration(txQE, record));
  }

  @Override
  public Future<Integer> calculateGeneration(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return txQE.query(dsl -> dsl.select(coalesce(max(RECORDS_LB.GENERATION), 0).as(RECORDS_LB.GENERATION))
      .from(RECORDS_LB.innerJoin(SNAPSHOTS_LB).on(RECORDS_LB.SNAPSHOT_ID.eq(SNAPSHOTS_LB.ID)))
      .where(RECORDS_LB.MATCHED_ID.eq(UUID.fromString(record.getMatchedId()))
        .and(SNAPSHOTS_LB.STATUS.eq(JobExecutionStatus.COMMITTED))
        .and(SNAPSHOTS_LB.UPDATED_DATE.lessThan(dsl.select(SNAPSHOTS_LB.PROCESSING_STARTED_DATE)
          .from(SNAPSHOTS_LB)
          .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(record.getSnapshotId())))))))
            .map(res -> res.get(RECORDS_LB.GENERATION));
  }

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> CompositeFuture.join(
      updateExternalIdsForRecord(txQE, record),
      LBParsedRecordDaoUtil.update(txQE, record.getParsedRecord())
    ).map(res -> record.getParsedRecord()));
  }

  @Override
  public Future<Optional<Record>> getRecordByExternalId(String externalId, ExternalIdType externalIdType,
      String tenantId) {
    Condition condition = LBRecordDaoUtil.getExternalIdCondition(externalId, externalIdType);
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.select(DSL.asterisk(), DSL.max(RECORDS_LB.GENERATION)
        .over(DSL.partitionBy(RECORDS_LB.MATCHED_ID)))
        .from(RECORDS_LB)
        .where(condition))
          .map(LBRecordDaoUtil::toRecord)
      .compose(record -> lookupAssociatedRecords(txQE, record, true)))
        .map(Optional::of);
  }

  @Override
  public Future<Record> saveUpdatedRecord(Record newRecord, Record oldRecord, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> saveUpdatedRecord(txQE, newRecord, oldRecord));
  }

  @Override
  public Future<Record> saveUpdatedRecord(ReactiveClassicGenericQueryExecutor txQE, Record newRecord, Record oldRecord) {
    return insertOrUpdateRecord(txQE, oldRecord).compose(r -> insertOrUpdateRecord(txQE, newRecord));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto,
      String tenantId) {
    Boolean suppressFromDiscovery = suppressFromDiscoveryDto.getSuppressFromDiscovery();
    String externalId = suppressFromDiscoveryDto.getId();
    String incomingIdType = suppressFromDiscoveryDto.getIncomingIdType().value();
    ExternalIdType externalIdType = ExternalIdType.valueOf(incomingIdType);
    Condition condition = LBRecordDaoUtil.getExternalIdCondition(externalId, externalIdType);
    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDaoUtil.findByCondition(txQE, condition)
      .compose(optionalRecord -> optionalRecord
        .map(record -> LBRecordDaoUtil.update(txQE, record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(suppressFromDiscovery))))
      .orElse(Future.failedFuture(new NotFoundException(String.format("Record with %s id: %s was not found", incomingIdType, externalId)))))
    ).map(u -> true);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return LBSnapshotDaoUtil.delete(getQueryExecutor(tenantId), snapshotId);
  }

  @Override
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, parsedRecordDto.getId())
      .compose(optionalRecord -> optionalRecord
        .map(existingRecord -> LBSnapshotDaoUtil.save(txQE, new Snapshot()
          .withJobExecutionId(snapshotId)
          .withStatus(Snapshot.Status.COMMITTED)) // no processing of the record is performed apart from the update itself
            .compose(s -> {
              Record newRecord = new Record()
                .withId(UUID.randomUUID().toString())
                .withSnapshotId(s.getJobExecutionId())
                .withMatchedId(parsedRecordDto.getId())
                .withRecordType(Record.RecordType.fromValue(parsedRecordDto.getRecordType().value()))
                .withParsedRecord(parsedRecordDto.getParsedRecord().withId(UUID.randomUUID().toString()))
                .withExternalIdsHolder(parsedRecordDto.getExternalIdsHolder())
                .withAdditionalInfo(parsedRecordDto.getAdditionalInfo())
                .withMetadata(parsedRecordDto.getMetadata())
                .withRawRecord(existingRecord.getRawRecord())
                .withOrder(existingRecord.getOrder())
                .withGeneration(existingRecord.getGeneration() + 1)
                .withState(Record.State.ACTUAL);
              return insertOrUpdateRecord(txQE, existingRecord.withState(Record.State.OLD))
                .compose(r -> insertOrUpdateRecord(txQE, newRecord));
            }))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Record with id '%s' was not found", parsedRecordDto.getId()))))
    ));
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private Record addToList(List<Record> records, Record record) {
    records.add(record);
    return record;
  }

  private SourceRecord addToList(List<SourceRecord> sourceRecords, SourceRecord sourceRecord) {
    sourceRecords.add(sourceRecord);
    return sourceRecord;
  }

  private Future<Optional<Record>> lookupAssociatedRecords(ReactiveClassicGenericQueryExecutor txQE, Optional<Record> record, boolean includeErrorRecord) {
    if (record.isPresent()) {
      return lookupAssociatedRecords(txQE, record.get(), includeErrorRecord).map(Optional::of);
    }
    return Future.succeededFuture(record);
  }

  private Future<Record> lookupAssociatedRecords(ReactiveClassicGenericQueryExecutor txQE, Record record, boolean includeErrorRecord) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = new ArrayList<>();
    futures.add(LBRawRecordDaoUtil.findById(txQE, record.getId()).onComplete(ar -> {
      if (ar.succeeded() && ar.result().isPresent()) {
        record.withRawRecord(ar.result().get());
      }
    }));
    futures.add(LBParsedRecordDaoUtil.findById(txQE, record.getId()).onComplete(ar -> {
      if (ar.succeeded() && ar.result().isPresent()) {
        record.withParsedRecord(ar.result().get());
      }
    }));
    if (includeErrorRecord) {
      futures.add(LBErrorRecordDaoUtil.findById(txQE, record.getId()).onComplete(ar -> {
        if (ar.succeeded() && ar.result().isPresent()) {
          record.withErrorRecord(ar.result().get());
        }
      }));
    }
    return CompositeFuture.all(futures).map(ar -> record);
  }

  private Future<Record> insertOrUpdateRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = new ArrayList<>();
    validateParsedRecordContent(record);
    if (Objects.nonNull(record.getRawRecord())) {
      futures.add(LBRawRecordDaoUtil.save(txQE, record.getRawRecord()));
    }
    if (Objects.nonNull(record.getParsedRecord())) {
      futures.add(LBParsedRecordDaoUtil.save(txQE, record.getParsedRecord()));
    }
    if (Objects.nonNull(record.getErrorRecord())) {
      futures.add(LBErrorRecordDaoUtil.save(txQE, record.getErrorRecord()));
    }
    return CompositeFuture.all(futures)
      .compose(res -> LBRecordDaoUtil.save(txQE, record));
  }

  private Future<Boolean> updateExternalIdsForRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return LBRecordDaoUtil.findById(txQE, record.getId())
      .map(optionalRecord -> {
        if (optionalRecord.isPresent()) {
          return optionalRecord;
        }
        String rollBackMessage = String.format("Record with id %s was not found", record.getId());
        throw new NotFoundException(rollBackMessage);
      })
      .map(Optional::get)
      .compose(persistedRecord -> {
        persistedRecord.withExternalIdsHolder(record.getExternalIdsHolder())
          .withMetadata(record.getMetadata());
        return LBRecordDaoUtil.update(txQE, persistedRecord)
          .map(update -> true);
      });
  }

  private void validateParsedRecordContent(Record record) {
    try {
      String parsedRecordContent = (String) record.getParsedRecord().getContent();
      record.getParsedRecord()
        .setFormattedContent(MarcUtil.marcJsonToTxtMarc(parsedRecordContent));
    } catch (IOException e) {
      LOG.error("Couldn't format MARC record", e);
      record.setErrorRecord(new ErrorRecord()
        .withId(record.getId())
        .withDescription(e.getMessage())
        .withContent(record.getParsedRecord().getContent()));
      record.setParsedRecord(null);
    }
  }

}