package org.folio.dao;

import static org.folio.dao.util.ErrorRecordDaoUtil.ERROR_RECORD_CONTENT;
import static org.folio.dao.util.ParsedRecordDaoUtil.PARSED_RECORD_CONTENT;
import static org.folio.dao.util.RawRecordDaoUtil.RAW_RECORD_CONTENT;
import static org.folio.rest.jooq.Tables.ERROR_RECORDS_LB;
import static org.folio.rest.jooq.Tables.RAW_RECORDS_LB;
import static org.folio.rest.jooq.Tables.RECORDS_LB;
import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.trueCondition;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang.ArrayUtils;
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
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.enums.RecordState;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Name;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.reactivex.Flowable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.sqlclient.Row;

@Component
public class RecordDaoImpl implements RecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(RecordDaoImpl.class);

  private static final String CTE = "cte";

  private static final String ID = "id";
  private static final String CONTENT = "content";
  private static final String COUNT = "count";
  private static final String TABLE_FIELD_TEMPLATE = "{0}.{1}";

  private static final Field<Integer> COUNT_FIELD = field(name(COUNT), Integer.class);

  private static final Field<?>[] RECORD_FIELDS = new Field<?>[] {
    RECORDS_LB.ID,
    RECORDS_LB.SNAPSHOT_ID,
    RECORDS_LB.MATCHED_ID,
    RECORDS_LB.GENERATION,
    RECORDS_LB.RECORD_TYPE,
    RECORDS_LB.INSTANCE_ID,
    RECORDS_LB.STATE,
    RECORDS_LB.LEADER_RECORD_STATUS,
    RECORDS_LB.ORDER,
    RECORDS_LB.SUPPRESS_DISCOVERY,
    RECORDS_LB.CREATED_BY_USER_ID,
    RECORDS_LB.CREATED_DATE,
    RECORDS_LB.UPDATED_BY_USER_ID,
    RECORDS_LB.UPDATED_DATE,
    RECORDS_LB.INSTANCE_HRID
  };

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
  public Future<RecordCollection> getRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.selectCount()
        .from(RECORDS_LB)
        .where(condition.and(filterRecordByType(recordType.name())))))
      .select(getAllRecordFieldsWithCount(prt))
        .from(RECORDS_LB)
        .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
        .leftJoin(RAW_RECORDS_LB).on(RECORDS_LB.ID.eq(RAW_RECORDS_LB.ID))
        .leftJoin(ERROR_RECORDS_LB).on(RECORDS_LB.ID.eq(ERROR_RECORDS_LB.ID))
        .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
        .where(condition.and(filterRecordByType(recordType.name())))
        .orderBy(orderFields)
        .offset(offset)
        .limit(limit)
    )).map(this::toRecordCollection);
  }

  @Override
  public Flowable<Record> streamRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name prt = name(recordType.getTableName());
    String sql = DSL.select(getAllRecordFields(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .leftJoin(RAW_RECORDS_LB).on(RECORDS_LB.ID.eq(RAW_RECORDS_LB.ID))
      .leftJoin(ERROR_RECORDS_LB).on(RECORDS_LB.ID.eq(ERROR_RECORDS_LB.ID))
      .where(condition.and(filterRecordByType(recordType.name())))
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit)
      .getSQL(ParamType.INLINED);
    return getCachecPool(tenantId).rxBegin()
      .flatMapPublisher(tx -> tx.rxPrepare(sql)
        .flatMapPublisher(pq -> pq.createStream(1)
          .toFlowable()
          .map(this::toRow)
          .map(this::toRecord))
        .doAfterTerminate(tx::commit));
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordById(txQE, id));
  }

  @Override
  public Future<Optional<Record>> getRecordById(ReactiveClassicGenericQueryExecutor txQE, String id) {
    Condition condition = RECORDS_LB.ID.eq(UUID.fromString(id));
    return getRecordByCondition(txQE, condition);
  }

  @Override
  public Future<Optional<Record>> getRecordByMatchedId(String matchedId, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByMatchedId(txQE, matchedId));
  }

  @Override
  public Future<Optional<Record>> getRecordByMatchedId(ReactiveClassicGenericQueryExecutor txQE, String id) {
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
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.selectCount()
        .from(RECORDS_LB)
        .where(condition.and(filterRecordByType(recordType.name()))
          .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull()))))
      .select(getRecordFieldsWithCount(prt))
      .from(RECORDS_LB)
      .innerJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
      .where(condition.and(filterRecordByType(recordType.name()))
        .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull()))
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit)
    )).map(this::toSourceRecordCollection);
  }

  @Override
  public Flowable<SourceRecord> streamSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    Name prt = name(recordType.getTableName());
    String sql = DSL.select(getRecordFields(prt))
      .from(RECORDS_LB)
      .innerJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .where(condition.and(filterRecordByType(recordType.name()))
        .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull()))
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit)
      .getSQL(ParamType.INLINED);
    return getCachecPool(tenantId).rxBegin()
      .flatMapPublisher(tx -> tx.rxPrepare(sql)
        .flatMapPublisher(pq -> pq.createStream(1)
          .toFlowable()
          .map(this::toRow)
          .map(this::toSourceRecord))
        .doAfterTerminate(tx::commit));
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(List<String> externalIds, ExternalIdType externalIdType, RecordType recordType, Boolean deleted, String tenantId) {
    Condition condition = RecordDaoUtil.getExternalIdsCondition(externalIds, externalIdType)
      .and(RecordDaoUtil.filterRecordByDeleted(deleted));
    Name cte = name(CTE);
    Name prt = name(recordType.getTableName());
    return getQueryExecutor(tenantId).transaction(txQE -> txQE.query(dsl -> dsl
      .with(cte.as(dsl.selectCount()
        .from(RECORDS_LB)
        .where(condition.and(filterRecordByType(recordType.name())))))
      .select(getRecordFieldsWithCount(prt))
      .from(RECORDS_LB)
      .leftJoin(table(prt)).on(RECORDS_LB.ID.eq(field(TABLE_FIELD_TEMPLATE, UUID.class, prt, name(ID))))
      .rightJoin(dsl.select().from(table(cte))).on(trueCondition())
      .where(condition.and(filterRecordByType(recordType.name()))
        .and(RECORDS_LB.LEADER_RECORD_STATUS.isNotNull()))
    )).map(this::toSourceRecordCollection);
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
    return getQueryExecutor(tenantId)
      .transaction(txQE -> getRecordByExternalId(txQE, externalId, externalIdType));
  }

  @Override
  public Future<Optional<Record>> getRecordByExternalId(ReactiveClassicGenericQueryExecutor txQE,
      String externalId, ExternalIdType externalIdType) {
    Condition condition = RecordDaoUtil.getExternalIdCondition(externalId, externalIdType);
    return txQE.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
      .where(condition)
      .orderBy(RECORDS_LB.GENERATION.sort(SortOrder.DESC))
      .limit(1))
        .map(RecordDaoUtil::toOptionalRecord)
        .compose(optionalRecord -> optionalRecord
          .map(record -> lookupAssociatedRecords(txQE, record, false).map(Optional::of))
          .orElse(Future.failedFuture(new NotFoundException(String.format("Record with %s id: %s was not found", externalIdType, externalId)))))
        .onFailure(v -> txQE.rollback());
  }

  @Override
  public Future<Record> saveUpdatedRecord(ReactiveClassicGenericQueryExecutor txQE, Record newRecord, Record oldRecord) {
    return insertOrUpdateRecord(txQE, oldRecord).compose(r -> insertOrUpdateRecord(txQE, newRecord));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, ExternalIdType externalIdType, Boolean suppress, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> getRecordByExternalId(txQE, id, externalIdType)
      .compose(optionalRecord -> optionalRecord
        .map(record -> RecordDaoUtil.update(txQE, record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(suppress))))
      .orElse(Future.failedFuture(new NotFoundException(String.format("Record with %s id: %s was not found", externalIdType, id))))))
        .map(u -> true);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return SnapshotDaoUtil.delete(getQueryExecutor(tenantId), snapshotId);
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private PgPool getCachecPool(String tenantId) {
    return postgresClientFactory.getCachedPool(tenantId);
  }

  private Row toRow(io.vertx.reactivex.sqlclient.Row row) {
    return row.getDelegate();
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
      String content = ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord());
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
          .withAdditionalInfo(record.getAdditionalInfo())
          .withMetadata(record.getMetadata());
        return RecordDaoUtil.update(txQE, persistedRecord)
          .map(update -> true);
      });
  }

  private Field<?>[] getRecordFields(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(RECORD_FIELDS, new Field<?>[] {
      field(TABLE_FIELD_TEMPLATE, JSONB.class, prt, name(CONTENT))
    });
  }

  private Field<?>[] getRecordFieldsWithCount(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(getRecordFields(prt), new Field<?>[] {
      COUNT_FIELD
    });
  }

  private Field<?>[] getAllRecordFields(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(RECORD_FIELDS, new Field<?>[] {
      field(TABLE_FIELD_TEMPLATE, JSONB.class, prt, name(CONTENT)).as(PARSED_RECORD_CONTENT),
      RAW_RECORDS_LB.CONTENT.as(RAW_RECORD_CONTENT),
      ERROR_RECORDS_LB.CONTENT.as(ERROR_RECORD_CONTENT),
      ERROR_RECORDS_LB.DESCRIPTION
    });
  }

  private Field<?>[] getAllRecordFieldsWithCount(Name prt) {
    return (Field<?>[]) ArrayUtils.addAll(getAllRecordFields(prt), new Field<?>[] {
      COUNT_FIELD
    });
  }

  private RecordCollection toRecordCollection(QueryResult result) {
    RecordCollection recordCollection = new RecordCollection().withTotalRecords(0);
    List<Record> records = result.stream().map(res -> asRow(res.unwrap())).map(row -> {
      recordCollection.setTotalRecords(row.getInteger(COUNT));
      return toRecord(row);
    }).collect(Collectors.toList());
    if (!records.isEmpty() && Objects.nonNull(records.get(0).getId())) {
      recordCollection.withRecords(records);
    }
    return recordCollection;
  }

  private SourceRecordCollection toSourceRecordCollection(QueryResult result) {
    SourceRecordCollection sourceRecordCollection = new SourceRecordCollection().withTotalRecords(0);
    List<SourceRecord> sourceRecords = result.stream().map(res -> asRow(res.unwrap())).map(row -> {
      sourceRecordCollection.setTotalRecords(row.getInteger(COUNT));
      return RecordDaoUtil.toSourceRecord(RecordDaoUtil.toRecord(row))
        .withParsedRecord(ParsedRecordDaoUtil.toParsedRecord(row));
    }).collect(Collectors.toList());
    if (!sourceRecords.isEmpty() && Objects.nonNull(sourceRecords.get(0).getRecordId())) {
      sourceRecordCollection.withSourceRecords(sourceRecords);
    }
    return sourceRecordCollection;
  }

  private SourceRecord toSourceRecord(Row row) {
    SourceRecord sourceRecord = RecordDaoUtil.toSourceRecord(row);
    ParsedRecord parsedRecord = ParsedRecordDaoUtil.toParsedRecord(row);
    if (Objects.nonNull(parsedRecord.getContent())) {
      sourceRecord.setParsedRecord(parsedRecord);
    }
    return sourceRecord;
  }

  private Record toRecord(Row row) {
    Record record = RecordDaoUtil.toRecord(row);
    RawRecord rawRecord = RawRecordDaoUtil.toJoinedRawRecord(row);
    if (Objects.nonNull(rawRecord.getContent())) {
      record.setRawRecord(rawRecord);
    }
    ParsedRecord parsedRecord = ParsedRecordDaoUtil.toJoinedParsedRecord(row);
    if (Objects.nonNull(parsedRecord.getContent())) {
      record.setParsedRecord(parsedRecord);
    }
    ErrorRecord errorRecord = ErrorRecordDaoUtil.toJoinedErrorRecord(row);
    if (Objects.nonNull(errorRecord.getContent())) {
      record.setErrorRecord(errorRecord);
    }
    return record;
  }

}
