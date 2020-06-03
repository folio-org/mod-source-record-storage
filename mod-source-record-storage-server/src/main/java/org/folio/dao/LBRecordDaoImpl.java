package org.folio.dao;

import static org.folio.rest.jooq.Tables.MARC_RECORDS_LB;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;
import static org.jooq.impl.DSL.coalesce;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.trueCondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

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
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
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
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;

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
  public <T> Future<T> executeInTransaction(Function<ReactiveClassicGenericQueryExecutor, Future<T>> action, String tenantId) {
    return getQueryExecutor(tenantId).transaction(action);
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      RecordCollection recordCollection = new RecordCollection();
      recordCollection.withRecords(new ArrayList<>());
      return CompositeFuture.all(
        LBRecordDaoUtil.streamByCondition(txQE, condition, orderFields, offset, limit)
          .compose(stream -> CompositeFuture.all(stream
            .map(pr -> lookupAssociatedRecords(txQE, pr, true)
            .map(r -> addToList(recordCollection.getRecords(), r)))
            .collect(Collectors.toList()))),
        LBRecordDaoUtil.countByCondition(txQE, condition)
          .map(totalRecords -> addTotalRecords(recordCollection, totalRecords))
      ).map(res -> recordCollection);
    });
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
    return getQueryExecutor(tenantId).transaction(txQE -> saveRecord(txQE, record));
  }

  @Override
  public Future<Record> saveRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return insertOrUpdateRecord(txQE, record);
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
    final String cteTableName = "cte";
    final String countColumn = "count";
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl.with(cteTableName).as(dsl.select()
      .from(RECORDS_LB)
      .innerJoin(MARC_RECORDS_LB).on(RECORDS_LB.ID.eq(MARC_RECORDS_LB.ID))
      .where(RECORDS_LB.STATE.eq(RecordState.ACTUAL).and(condition))).select()
        .from(dsl.select().from(table(name(cteTableName))).offset(offset).limit(limit))
        .rightJoin(dsl.selectCount().from(table(name(cteTableName)))).on(trueCondition())
    )).map(res -> {
      SourceRecordCollection sourceRecordCollection = new SourceRecordCollection();
      List<SourceRecord> sourceRecords = res.stream().map(r -> asRow(r.unwrap())).map(row -> {
        sourceRecordCollection.setTotalRecords(row.getInteger(countColumn));
        return LBRecordDaoUtil.toSourceRecord(LBRecordDaoUtil.toRecord(row))
          .withParsedRecord(LBParsedRecordDaoUtil.toParsedRecord(row));
      }).collect(Collectors.toList());
      if (Objects.nonNull(sourceRecords.get(0).getRecordId())) {
        sourceRecordCollection.withSourceRecords(sourceRecords);
      }
      return sourceRecordCollection;
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
  public Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
        .where(condition))
          .map(LBRecordDaoUtil::toOptionalRecord)
      .compose(optionalRecord -> {
        if (optionalRecord.isPresent()) {
          return lookupAssociatedRecords(txQE, optionalRecord.get(), false)
            .map(LBRecordDaoUtil::toSourceRecord)
            .map(sourceRecord -> {
              if (Objects.nonNull(sourceRecord.getParsedRecord())) {
                return Optional.of(sourceRecord);
              }
              return Optional.empty();
            });
        }
        return Future.succeededFuture(Optional.empty());
      }));
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
    return getQueryExecutor(tenantId).transaction(txQE -> CompositeFuture.all(
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

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private Record addToList(List<Record> records, Record record) {
    records.add(record);
    return record;
  }

  private RecordCollection addTotalRecords(RecordCollection recordCollection, Integer totalRecords) {
    return recordCollection.withTotalRecords(totalRecords);
  }

  private Row asRow(Row row) {
    return row;
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
    futures.add(LBRawRecordDaoUtil.findById(txQE, record.getId()).map(rr -> {
      if (rr.isPresent()) {
        record.withRawRecord(rr.get());
      }
      return record;
    }));
    futures.add(LBParsedRecordDaoUtil.findById(txQE, record.getId()).map(pr -> {
      if (pr.isPresent()) {
        record.withParsedRecord(pr.get());
      }
      return record;
    }));
    if (includeErrorRecord) {
      futures.add(LBErrorRecordDaoUtil.findById(txQE, record.getId()).map(er -> {
        if (er.isPresent()) {
          record.withErrorRecord(er.get());
        }
        return record;
      }));
    }
    return CompositeFuture.all(futures).map(res -> record);
  }

  private Future<Record> insertOrUpdateRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = new ArrayList<>();
    if (Objects.nonNull(record.getRawRecord())) {
      futures.add(LBRawRecordDaoUtil.save(txQE, record.getRawRecord()));
    }
    if (Objects.nonNull(record.getParsedRecord())) {
      validateParsedRecordContent(record);
      futures.add(LBParsedRecordDaoUtil.save(txQE, record.getParsedRecord()));
    }
    if (Objects.nonNull(record.getErrorRecord())) {
      futures.add(LBErrorRecordDaoUtil.save(txQE, record.getErrorRecord()));
    }
    return CompositeFuture.all(futures)
      .compose(res -> LBRecordDaoUtil.save(txQE, record)).map(r -> {
        if (Objects.nonNull(record.getRawRecord())) {
          r.withRawRecord(record.getRawRecord());
        }
        if (Objects.nonNull(record.getParsedRecord())) {
          r.withParsedRecord(record.getParsedRecord());
        }
        if (Objects.nonNull(record.getErrorRecord())) {
          r.withErrorRecord(record.getErrorRecord());
        }
        return r;
      });
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
      record.getParsedRecord()
        .setFormattedContent(MarcUtil.marcJsonToTxtMarc((String) record.getParsedRecord().getContent()));
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