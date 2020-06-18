package org.folio.dao;

import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.trueCondition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.folio.dao.util.ErrorRecordDaoUtil;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.MarcUtil;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RawRecordDaoUtil;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.enums.RecordState;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;

@Component
public class RecordDaoImpl implements RecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(RecordDaoImpl.class);

  private static final String CTE_TABLE_NAME = "cte";
  private static final String CTEOP_TABLE_NAME = "cteop";
  private static final String ID_COLUMN = "id";
  private static final String COUNT_COLUMN = "count";
  private static final String TABLE_FIELD = "{0}.{1}";

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public RecordDaoImpl(final PostgresClientFactory postgresClientFactory) {
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
        RecordDaoUtil.streamByCondition(txQE, condition, orderFields, offset, limit)
          .compose(stream -> CompositeFuture.all(stream
            .map(pr -> lookupAssociatedRecords(txQE, pr, true)
            .map(r -> addToList(recordCollection.getRecords(), r)))
            .collect(Collectors.toList()))),
        RecordDaoUtil.countByCondition(txQE, condition)
          .map(totalRecords -> addTotalRecords(recordCollection, totalRecords))
      ).map(res -> recordCollection);
    });
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, id));
  }

  @Override
  public Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String id) {
    Condition condition = RECORDS_LB.MATCHED_ID.eq(UUID.fromString(id))
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL)
      .or(RECORDS_LB.STATE.eq(RecordState.DELETED)));
    return getRecordByCondition(txQE, condition);
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByCondition(txQE, condition));
  }

  @Override
  public Future<Optional<Record>> getRecordByCondition(ReactiveClassicGenericQueryExecutor txQE, Condition condition) {
    return RecordDaoUtil.findByCondition(txQE, condition)
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
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, record.getId())
      .compose(optionalRecord -> optionalRecord
        .map(r -> saveRecord(txQE, record))
        .orElse(Future.failedFuture(new NotFoundException(String.format("Record with id '%s' was not found", record.getId()))))));
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    // NOTE: currently only record type available is MARC
    // having a dedicated table per record type has some complications
    // if a new record type is added, will have two options to continue using single query to fetch source records
    // 1. add record type query parameter to endpoint and join with table for that record type
    //    - this will not afford source record request returning heterogeneous record types
    // 2. join on every record type table
    //    - this could present performance issues

    RecordType recordType = RecordType.MARC;
    Name id = name(ID_COLUMN);
    Name cte = name(CTE_TABLE_NAME);
    Name cteop = name(CTEOP_TABLE_NAME);
    Name prt = name(recordType.getTableName());
    Field<UUID> recordIdField = field(TABLE_FIELD, UUID.class, cteop, id);
    Field<UUID> parsedRecordIdField = field(TABLE_FIELD, UUID.class, prt, id);
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.select()
        .from(RECORDS_LB)
        .where(condition.and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull()))))
      .with(cteop.as(dsl.select()
        // Unfortunately, cannot use .from(table(cte)) here.
        // It seems to be a bug with jOOQ, but seems to be optimized out to not execute select twice.
        .from(RECORDS_LB)
        .where(condition.and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull()))
        .orderBy(orderFields)
        .offset(offset)
        .limit(limit)))
      .select()
        .from(table(cteop))
        .innerJoin(table(prt)).on(recordIdField.eq(parsedRecordIdField))
        .rightJoin(dsl.selectCount().from(table(cte))).on(trueCondition())
    )).map(res -> {
      SourceRecordCollection sourceRecordCollection = new SourceRecordCollection();
      List<SourceRecord> sourceRecords = res.stream().map(r -> asRow(r.unwrap())).map(row -> {
        sourceRecordCollection.setTotalRecords(row.getInteger(COUNT_COLUMN));
        return RecordDaoUtil.toSourceRecord(RecordDaoUtil.toRecord(row))
          .withParsedRecord(ParsedRecordDaoUtil.toParsedRecord(row));
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
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL))
      .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull());
    return getSourceRecordByCondition(condition, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByExternalId(String externalId, ExternalIdType externalIdType, String tenantId) {
    Condition condition = RecordDaoUtil.getExternalIdCondition(externalId, externalIdType)
      .and(RECORDS_LB.STATE.eq(RecordState.ACTUAL))
      .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull());
    return getSourceRecordByCondition(condition, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordByCondition(Condition condition, String tenantId) {
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
        .where(condition))
          .map(RecordDaoUtil::toOptionalRecord)
      .compose(optionalRecord -> {
        if (optionalRecord.isPresent()) {
          return lookupAssociatedRecords(txQE, optionalRecord.get(), false)
            .map(RecordDaoUtil::toSourceRecord)
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
    return txQE.query(dsl -> dsl.select(max(RECORDS_LB.GENERATION).as(RECORDS_LB.GENERATION))
      .from(RECORDS_LB.innerJoin(SNAPSHOTS_LB).on(RECORDS_LB.SNAPSHOT_ID.eq(SNAPSHOTS_LB.ID)))
      .where(RECORDS_LB.MATCHED_ID.eq(UUID.fromString(record.getMatchedId()))
        .and(SNAPSHOTS_LB.STATUS.eq(JobExecutionStatus.COMMITTED))
        .and(SNAPSHOTS_LB.UPDATED_DATE.lessThan(dsl.select(SNAPSHOTS_LB.PROCESSING_STARTED_DATE)
          .from(SNAPSHOTS_LB)
          .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(record.getSnapshotId())))))))
            .map(res -> {
              Integer generation = res.get(RECORDS_LB.GENERATION);
              return Objects.nonNull(generation) ? ++generation : 0;
            });
  }

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> CompositeFuture.all(
      updateExternalIdsForRecord(txQE, record),
      ParsedRecordDaoUtil.update(txQE, record.getParsedRecord(), ParsedRecordDaoUtil.toRecordType(record))
    ).map(res -> record.getParsedRecord()));
  }

  @Override
  public Future<Optional<Record>> getRecordByExternalId(String externalId, ExternalIdType externalIdType,
      String tenantId) {
    Condition condition = RecordDaoUtil.getExternalIdCondition(externalId, externalIdType);
    return getQueryExecutor(tenantId)
      .transaction(txQE -> txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
        .where(condition)
        .orderBy(RECORDS_LB.GENERATION.sort(SortOrder.ASC))
        .limit(1))
          .map(RecordDaoUtil::toOptionalRecord)
          .compose(optionalRecord -> optionalRecord
            .map(record -> lookupAssociatedRecords(txQE, record, true).map(Optional::of))
          .orElse(Future.failedFuture(new NotFoundException(String.format("Record with %s id: %s was not found", externalIdType, externalId))))));
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
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, String idType, Boolean suppress, String tenantId) {
    ExternalIdType externalIdType = RecordDaoUtil.toExternalIdType(idType);
    Condition condition = RecordDaoUtil.getExternalIdCondition(id, externalIdType);
    return getQueryExecutor(tenantId).transaction(txQE -> RecordDaoUtil.findByCondition(txQE, condition)
      .compose(optionalRecord -> optionalRecord
        .map(record -> RecordDaoUtil.update(txQE, record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(suppress))))
      .orElse(Future.failedFuture(new NotFoundException(String.format("Record with %s id: %s was not found", idType, id))))))
        .map(u -> true);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return SnapshotDaoUtil.delete(getQueryExecutor(tenantId), snapshotId);
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
    futures.add(RawRecordDaoUtil.findById(txQE, record.getId()).map(rr -> {
      if (rr.isPresent()) {
        record.withRawRecord(rr.get());
      }
      return record;
    }));
    futures.add(ParsedRecordDaoUtil.findById(txQE, record.getId(), ParsedRecordDaoUtil.toRecordType(record)).map(pr -> {
      if (pr.isPresent()) {
        record.withParsedRecord(pr.get());
      }
      return record;
    }));
    if (includeErrorRecord) {
      futures.add(ErrorRecordDaoUtil.findById(txQE, record.getId()).map(er -> {
        if (er.isPresent()) {
          record.withErrorRecord(er.get());
        }
        return record;
      }));
    }
    return CompositeFuture.all(futures).map(res -> record);
  }

  private Future<Record> insertOrUpdateRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return RawRecordDaoUtil.save(txQE, record.getRawRecord())
      .compose(rawRecord -> {
        if (Objects.nonNull(record.getParsedRecord())) {
          return insertOrUpdateParsedRecord(txQE, record);
        }
        return Future.succeededFuture(null);
      })
      .compose(parsedRecord -> {
        if (Objects.nonNull(record.getErrorRecord())) {
          return ErrorRecordDaoUtil.save(txQE, record.getErrorRecord());
        }
        return Future.succeededFuture(null);
      })
      .compose(errorRecord -> RecordDaoUtil.save(txQE, record)).map(savedRecord -> {
        if (Objects.nonNull(record.getRawRecord())) {
          savedRecord.withRawRecord(record.getRawRecord());
        }
        if (Objects.nonNull(record.getParsedRecord())) {
          savedRecord.withParsedRecord(record.getParsedRecord());
        }
        if (Objects.nonNull(record.getErrorRecord())) {
          savedRecord.withErrorRecord(record.getErrorRecord());
        }
        return savedRecord;
      });
  }

  private Future<ParsedRecord> insertOrUpdateParsedRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    try {
      String content = (String) ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord()).getContent();
      record.getParsedRecord().setFormattedContent(MarcUtil.marcJsonToTxtMarc(content));
      return ParsedRecordDaoUtil.save(txQE, record.getParsedRecord(), ParsedRecordDaoUtil.toRecordType(record))
        .map(parsedRecord -> {
          record.withLeaderRecordStatus(ParsedRecordDaoUtil.getLeaderStatus(record.getParsedRecord()));
          return parsedRecord;
        });
    } catch (Exception e) {
      LOG.error("Couldn't format MARC record", e);
      record.withErrorRecord(new ErrorRecord()
        .withId(record.getId())
        .withDescription(e.getMessage())
        .withContent(record.getParsedRecord().getContent()));
      record.withParsedRecord(null)
        .withLeaderRecordStatus(null);
      return Future.succeededFuture(null);
    }
  }

  private Future<Boolean> updateExternalIdsForRecord(ReactiveClassicGenericQueryExecutor txQE, Record record) {
    return RecordDaoUtil.findById(txQE, record.getId())
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
        return RecordDaoUtil.update(txQE, persistedRecord)
          .map(update -> true);
      });
  }

}