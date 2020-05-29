package org.folio.dao;

import static org.folio.rest.jooq.Tables.ERROR_RECORDS_LB;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.ErrorRecordsLb;
import org.folio.rest.jooq.tables.records.ErrorRecordsLbRecord;
import org.jooq.Condition;
import org.jooq.InsertSetStep;
import org.jooq.InsertValuesStepN;
import org.jooq.OrderField;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class LBErrorRecordDao {

  public static Future<List<ErrorRecord>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition,
      Collection<OrderField<?>> orderFields, int offset, int limit) {
    return queryExecutor.executeAny(dsl -> dsl.selectFrom(ERROR_RECORDS_LB)
      .where(condition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit))
        .map(LBErrorRecordDao::toErrorRecords);
  }

  public static Future<Optional<ErrorRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(ERROR_RECORDS_LB)
      .where(ERROR_RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(LBErrorRecordDao::toOptionalErrorRecord);
  }

  public static Future<ErrorRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor, ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = toDatabaseErrorRecord(errorRecord);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(ERROR_RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(LBErrorRecordDao::toErrorRecord);
  }

  public static Future<List<ErrorRecord>> save(ReactiveClassicGenericQueryExecutor queryExecutor, List<ErrorRecord> snapshots) {
    return queryExecutor.executeAny(dsl -> {
      InsertSetStep<ErrorRecordsLbRecord> insertSetStep = dsl.insertInto(ERROR_RECORDS_LB);
      InsertValuesStepN<ErrorRecordsLbRecord> insertValuesStepN = null;
      for (ErrorRecord ErrorRecord : snapshots) {
          insertValuesStepN = insertSetStep.values(toDatabaseErrorRecord(ErrorRecord).intoArray());
      }
      return insertValuesStepN;
    }).map(LBErrorRecordDao::toErrorRecords);
  }

  public static Future<ErrorRecord> update(ReactiveClassicGenericQueryExecutor queryExecutor, ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = toDatabaseErrorRecord(errorRecord);
    return queryExecutor.executeAny(dsl -> dsl.update(ERROR_RECORDS_LB)
      .set(dbRecord)
      .where(ERROR_RECORDS_LB.ID.eq(UUID.fromString(errorRecord.getId())))
      .returning())
        .map(LBErrorRecordDao::toOptionalErrorRecord)
        .map(optionalErrorRecord -> {
          if (optionalErrorRecord.isPresent()) {
            return optionalErrorRecord.get();
          }
          throw new RuntimeException(String.format("ErrorRecord with id '%s' was not found", errorRecord.getId()));
        });
  }

  public static Future<Boolean> delete(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(ERROR_RECORDS_LB)
      .where(ERROR_RECORDS_LB.ID.eq(UUID.fromString(id))))
      .map(res -> res == 1);
  }

  public static ErrorRecord toErrorRecord(Row row) {
    ErrorRecordsLb pojo = RowMappers.getErrorRecordsLbMapper().apply(row);
    return new ErrorRecord()
      .withId(pojo.getId().toString())
      .withContent(pojo.getContent())
      .withDescription(pojo.getDescription());
  }

  public static ErrorRecordsLbRecord toDatabaseErrorRecord(ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = new ErrorRecordsLbRecord();
    if (StringUtils.isNotEmpty(errorRecord.getId())) {
      dbRecord.setId(UUID.fromString(errorRecord.getId()));
    }
    if (Objects.nonNull(errorRecord.getContent())) {
      dbRecord.setContent((String) errorRecord.getContent());
    }
    dbRecord.setDescription(errorRecord.getDescription());
    return dbRecord;
  }

  private static ErrorRecord toErrorRecord(RowSet<Row> rows) {
    return toErrorRecord(rows.iterator().next());
  }

  private static List<ErrorRecord> toErrorRecords(RowSet<Row> rows) {
    return StreamSupport.stream(rows.spliterator(), false).map(LBErrorRecordDao::toErrorRecord).collect(Collectors.toList());
  }

  private static Optional<ErrorRecord> toOptionalErrorRecord(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toErrorRecord(rows.iterator().next())) : Optional.empty();
  }

  private static Optional<ErrorRecord> toOptionalErrorRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toErrorRecord(row)) : Optional.empty();
  }

}