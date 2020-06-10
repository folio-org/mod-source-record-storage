package org.folio.dao.util;

import static org.folio.rest.jooq.Tables.ERROR_RECORDS_LB;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.ErrorRecordsLb;
import org.folio.rest.jooq.tables.records.ErrorRecordsLbRecord;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

/**
 * Utility class for managing {@link ErrorRecord}
 */
public final class ErrorRecordDaoUtil {

  private ErrorRecordDaoUtil() { }

  /**
   * Searches for {@link ErrorRecord} by id using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param id            id
   * @return future with optional ErrorRecord
   */
  public static Future<Optional<ErrorRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(ERROR_RECORDS_LB)
      .where(ERROR_RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(ErrorRecordDaoUtil::toOptionalErrorRecord);
  }

  /**
   * Saves {@link ErrorRecord} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param errorRecord   error record
   * @return future with updated ErrorRecord
   */
  public static Future<ErrorRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor, ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = toDatabaseErrorRecord(errorRecord);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(ERROR_RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(ErrorRecordDaoUtil::toSingleErrorRecord);
  }

  /**
   * Convert database query result {@link Row} to {@link ErrorRecord}
   * 
   * @param row query result row
   * @return ErrorRecord
   */
  public static ErrorRecord toErrorRecord(Row row) {
    ErrorRecordsLb pojo = RowMappers.getErrorRecordsLbMapper().apply(row);
    return new ErrorRecord()
      .withId(pojo.getId().toString())
      .withContent(pojo.getContent())
      .withDescription(pojo.getDescription());
  }

  /**
   * Convert database query result {@link Row} to {@link Optional} {@link ErrorRecord}
   * 
   * @param row query result row
   * @return optional ErrorRecord
   */
  public static Optional<ErrorRecord> toOptionalErrorRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toErrorRecord(row)) : Optional.empty();
  }

  /**
   * Convert {@link ErrorRecord} to database record {@link ErrorRecordsLbRecord}
   * 
   * @param errorRecord error record
   * @return ErrorRecordsLbRecord
   */
  public static ErrorRecordsLbRecord toDatabaseErrorRecord(ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = new ErrorRecordsLbRecord();
    if (StringUtils.isNotEmpty(errorRecord.getId())) {
      dbRecord.setId(UUID.fromString(errorRecord.getId()));
    }
    if (Objects.nonNull(errorRecord.getContent())) {
      if (errorRecord.getContent() instanceof String) {
        dbRecord.setContent((String) errorRecord.getContent());
      } else {
        dbRecord.setContent(JsonObject.mapFrom(errorRecord.getContent()).encode());
      }
    }
    dbRecord.setDescription(errorRecord.getDescription());
    return dbRecord;
  }

  private static ErrorRecord toSingleErrorRecord(RowSet<Row> rows) {
    return toErrorRecord(rows.iterator().next());
  }

}