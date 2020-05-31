package org.folio.services;

import static java.lang.String.format;
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

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBErrorRecordDao;
import org.folio.dao.LBParsedRecordDao;
import org.folio.dao.LBRawRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.LBSnapshotDaoUtil;
import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
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
import org.springframework.stereotype.Service;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
@ConditionalOnProperty(prefix = "jooq", name = "services.record", havingValue = "true")
public class LBRecordServiceImpl implements LBRecordService {

  private static final Logger LOG = LoggerFactory.getLogger(LBRecordServiceImpl.class);

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public LBRecordServiceImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDao.findByCondition(txQE, condition)
      .compose(record -> lookupAssociatedRecords(txQE, record)));
  }

  public Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition) {
    return LBRecordDao.findByCondition(txQE, condition)
    .compose(record -> lookupAssociatedRecords(txQE, record));
  }

  @Override
  public Future<Optional<Record>> getRecordById(String matchedId, String tenantId) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(matchedId))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    return getRecordByCondition(condition, tenantId);
  }

  public Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String matchedId) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(matchedId))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    return getRecordByCondition(txQE, condition);
  }

  @Override
  public Future<Record> saveRecord(Record record, String tenantId) {
    if (Objects.isNull(record.getId())) {
      record.setId(UUID.randomUUID().toString());
    }
    if (Objects.isNull(record.getAdditionalInfo()) || Objects.isNull(record.getAdditionalInfo().getSuppressDiscovery())) {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false));
    }
    return getQueryExecutor(tenantId).transaction(txQE -> LBSnapshotDaoUtil.findById(txQE, record.getSnapshotId())
      .map(optionalSnapshot -> optionalSnapshot
          .orElseThrow(() -> new NotFoundException("Couldn't find snapshot with id " + record.getSnapshotId())))
      .compose(snapshot -> {
          if (Objects.isNull(snapshot.getProcessingStartedDate())) {
          String msgTemplate = "Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s";
          String message = format(msgTemplate, snapshot.getStatus());
          return Future.failedFuture(new BadRequestException(message));
          }
          return Future.succeededFuture();
      }).compose(v -> {
          if (record.getGeneration() == null) {
          return getRecordGeneration(txQE, record);
          }
          return Future.succeededFuture(record.getGeneration());
      }).compose(generation -> insertOrUpdateRecord(txQE, ensureRecordForeignKeys(record)
          .withGeneration(generation))));
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = recordCollection.getRecords().stream()
      .map(record -> saveRecord(record, tenantId))
      .collect(Collectors.toList());
    Promise<RecordsBatchResponse> promise = Promise.promise();
    CompositeFuture.join(futures).onComplete(ar -> {
      RecordsBatchResponse response = new RecordsBatchResponse();
      futures.forEach(save -> {
        if (save.failed()) {
          response.getErrorMessages().add(save.cause().getMessage());
        } else {
          response.getRecords().add((Record) save.result());
        }
      });
      response.setTotalRecords(response.getRecords().size());
      promise.complete(response);
    });
    return promise.future();
  }

  @Override
  public Future<Record> updateRecord(Record record, String tenantId) {
    return getRecordById(record.getId(), tenantId)
      .compose(optionalRecord -> optionalRecord
        .map(r -> saveRecord(ensureRecordForeignKeys(record), tenantId))
        .orElse(Future.failedFuture(new NotFoundException(format("Record with id '%s' was not found", record.getId())))));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.select(DSL.asterisk(), DSL.max(RECORDS_LB.GENERATION)
        .over(DSL.partitionBy(RECORDS_LB.MATCHED_ID)))
        .from(RECORDS_LB)
        .where(condition))
          .map(LBRecordDao::toRecord)
      .compose(record -> lookupAssociatedRecords(txQE, record)))
        .map(LBRecordDao::toSourceRecord)
        .map(Optional::of);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, String idType, String tenantId) {
    Condition condition = RECORDS_LB.INSTANCE_ID.eq(UUID.fromString(id));
    return getSourceRecordByCondition(condition, tenantId);
  }

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> CompositeFuture.join(
      updateExternalIdsForRecord(txQE, record),
      LBParsedRecordDao.update(txQE, record.getParsedRecord())
    ).map(res -> record.getParsedRecord()));
  }

  private Future<Boolean> updateExternalIdsForRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return LBRecordDao.findById(txQE, record.getId())
      .map(optionalRecord -> {
        if (optionalRecord.isPresent()) {
          return optionalRecord;
        }
        String rollBackMessage = format("Record with id %s was not found", record.getId());
        throw new NotFoundException(rollBackMessage);
      })
      .map(Optional::get)
      .compose(persistedRecord -> {
        persistedRecord.withExternalIdsHolder(record.getExternalIdsHolder())
          .withMetadata(record.getMetadata());
        return LBRecordDao.update(txQE, persistedRecord)
          .map(update -> true);
      });
  }

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = recordCollection.getRecords().stream()
      .map(this::validateParsedRecordId)
      .map(record -> updateParsedRecord(record, tenantId))
      .collect(Collectors.toList());
    Promise<ParsedRecordsBatchResponse> promise = Promise.promise();
    CompositeFuture.join(futures).onComplete(ar -> {
      ParsedRecordsBatchResponse response = new ParsedRecordsBatchResponse();
      futures.forEach(update -> {
        if (update.failed()) {
          response.getErrorMessages().add(update.cause().getMessage());
        } else {
          response.getParsedRecords().add((ParsedRecord) update.result());
        }
      });
      response.setTotalRecords(response.getParsedRecords().size());
      promise.complete(response);
    });
    return promise.future();
  }

  // TODO: run transactional
  // add dao to find by externalIdType
  @Override
  public Future<Record> getFormattedRecord(String externalIdIdentifier, String id, String tenantId) {
    Future<Optional<Record>> future;
    if (StringUtils.isNotEmpty(externalIdIdentifier)) {
      Condition condition = RECORDS_LB.INSTANCE_ID.eq(UUID.fromString(id))
        .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));
      future = getRecordByCondition(condition, tenantId);
    } else {
      future = getRecordById(id, tenantId);
    }
    return future.map(optionalRecord -> formatMarcRecord(optionalRecord.orElseThrow(() -> new NotFoundException(
      format("Couldn't find Record with %s id %s", externalIdIdentifier, id)))));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto,
      String tenantId) {

    String rollBackMessage = format("Record with %s id: %s was not found", suppressFromDiscoveryDto.getIncomingIdType().name(), suppressFromDiscoveryDto.getId());

    Boolean suppressFromDiscovery = suppressFromDiscoveryDto.getSuppressFromDiscovery();
    // ExternalIdType externalIdType = ExternalIdType.valueOf(suppressFromDiscoveryDto.getIncomingIdType().value());

    String id = suppressFromDiscoveryDto.getId();
    // String externalIdIdentifier = externalIdType.getExternalIdField();

    // TODO: add dao to find by externalIdType
    Condition condition = RECORDS_LB.INSTANCE_ID.eq(UUID.fromString(id))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL));

    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDao.findByCondition(txQE, condition)
      .compose(optionalRecord -> optionalRecord
        .map(record -> LBRecordDao.update(txQE, record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(suppressFromDiscovery))))
      .orElse(Future.failedFuture(new NotFoundException(rollBackMessage))))
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
          format("Record with id '%s' was not found", parsedRecordDto.getId()))))
    ));
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset,
      int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDao.streamByCondition(txQE, condition, orderFields, offset, limit)
      .compose(stream -> {
        List<Record> records = new ArrayList<>();
        Promise<List<Record>> promise = Promise.promise();
        CompositeFuture.all(stream
          .map(pr -> lookupAssociatedRecords(txQE, pr)
          .map(r -> addToList(records, r)))
          .collect(Collectors.toList()))
            .onComplete(res -> promise.complete(records));
        return promise.future();
    })).map(records -> new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size()));
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> LBRecordDao.streamByCondition(txQE, condition, orderFields, offset, limit)
      .compose(stream -> {
        List<SourceRecord> sourceRecords = new ArrayList<>();
        Promise<List<SourceRecord>> promise = Promise.promise();
        CompositeFuture.all(stream
          .map(pr -> lookupAssociatedRecords(txQE, pr)
          .map(LBRecordDao::toSourceRecord)
          .map(sr -> addToList(sourceRecords, sr)))
          .collect(Collectors.toList()))
            .onComplete(res -> promise.complete(sourceRecords));
        return promise.future();
    })).map(records -> new SourceRecordCollection()
      .withSourceRecords(records)
      .withTotalRecords(records.size()));
  }

  private Record addToList(List<Record> records, Record record) {
    records.add(record);
    return record;
  }

  private SourceRecord addToList(List<SourceRecord> sourceRecords, SourceRecord sourceRecord) {
    sourceRecords.add(sourceRecord);
    return sourceRecord;
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private Future<Optional<Record>> lookupAssociatedRecords(ReactiveClassicGenericQueryExecutor txQE, Optional<Record> record) {
    if (record.isPresent()) {
      return lookupAssociatedRecords(txQE, record.get())
        .map(Optional::of);
    }
    return Future.succeededFuture(record);
  }

  private Future<Record> lookupAssociatedRecords(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return CompositeFuture.all(LBRawRecordDao.findById(txQE, record.getId()).onComplete(ar -> {
      if (ar.succeeded() && ar.result().isPresent()) {
        record.withRawRecord(ar.result().get());
      }
    }), LBParsedRecordDao.findById(txQE, record.getId()).onComplete(ar -> {
      if (ar.succeeded() && ar.result().isPresent()) {
        record.withParsedRecord(ar.result().get());
      }
    }), LBErrorRecordDao.findById(txQE, record.getId()).onComplete(ar -> {
      if (ar.succeeded() && ar.result().isPresent()) {
        record.withErrorRecord(ar.result().get());
      }
    })).map(ar -> record);
  }

  private Future<Integer> getRecordGeneration(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return txQE.query(dsl -> dsl.select(coalesce(max(RECORDS_LB.GENERATION), 0).as(RECORDS_LB.GENERATION))
      .from(RECORDS_LB.join(SNAPSHOTS_LB).on(RECORDS_LB.SNAPSHOT_ID.eq(SNAPSHOTS_LB.ID)))
      .where(RECORDS_LB.MATCHED_ID.eq(UUID.fromString(record.getMatchedId()))
        .and(SNAPSHOTS_LB.STATUS.eq(JobExecutionStatus.COMMITTED))
        .and(SNAPSHOTS_LB.UPDATED_DATE.lessThan(dsl.select(SNAPSHOTS_LB.PROCESSING_STARTED_DATE)
          .from(SNAPSHOTS_LB)
          .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(record.getSnapshotId())))))))
            .map(res -> res.get(RECORDS_LB.GENERATION));
  }

  private Future<Record> insertOrUpdateRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    List<Future> futures = new ArrayList<>();
    validateParsedRecordContent(record);
    if (Objects.nonNull(record.getRawRecord())) {
      futures.add(LBRawRecordDao.save(txQE, record.getRawRecord()));
    }
    if (Objects.nonNull(record.getParsedRecord())) {
      futures.add(LBParsedRecordDao.save(txQE, record.getParsedRecord()));
    }
    if (Objects.nonNull(record.getErrorRecord())) {
      futures.add(LBErrorRecordDao.save(txQE, record.getErrorRecord()));
    }
    return CompositeFuture.all(futures)
      .compose(res -> LBRecordDao.save(txQE, record));
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

  private Record ensureRecordForeignKeys(Record record) {
    if (Objects.nonNull(record.getRawRecord()) && StringUtils.isEmpty(record.getRawRecord().getId())) {
      record.getRawRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getParsedRecord()) && StringUtils.isEmpty(record.getParsedRecord().getId())) {
      record.getParsedRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getErrorRecord()) && StringUtils.isEmpty(record.getErrorRecord().getId())) {
      record.getErrorRecord().setId(record.getId());
    }
    return record;
  }

  private Record validateParsedRecordId(Record record) {
    if (Objects.isNull(record.getParsedRecord()) && Objects.isNull(record.getParsedRecord().getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
    return record;
  }

  private Record formatMarcRecord(Record record) {
    try {
      String parsedRecordContent = (String) record.getParsedRecord().getContent();
      record.getParsedRecord()
        .setFormattedContent(MarcUtil.marcJsonToTxtMarc(parsedRecordContent));
    } catch (IOException e) {
      LOG.error("Couldn't format MARC record", e);
    }
    return record;
  }

}