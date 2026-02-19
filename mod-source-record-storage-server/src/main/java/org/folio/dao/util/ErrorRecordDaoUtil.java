package org.folio.dao.util;

import static org.folio.rest.jooq.Tables.ERROR_RECORDS_LB;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.util.executor.QueryExecutor;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jooq.tables.records.ErrorRecordsLbRecord;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.jooq.Record;

/**
 * Utility class for managing {@link ErrorRecord}
 */
public final class ErrorRecordDaoUtil {

  private static final String ID = "id";
  private static final String DESCRIPTION = "description";

  public static final String ERROR_RECORD_CONTENT = "error_record_content";

  private ErrorRecordDaoUtil() { }

  /**
   * Searches for {@link ErrorRecord} by id using {@link QueryExecutor}
   *
   * @param queryExecutor query executor
   * @param id            id
   * @return future with optional ErrorRecord
   */
  public static Future<Optional<ErrorRecord>> findById(QueryExecutor queryExecutor, String id) {
    return queryExecutor.execute(dsl -> dsl.selectFrom(ERROR_RECORDS_LB)
        .where(ERROR_RECORDS_LB.ID.eq(UUID.fromString(id))))
      .map(ErrorRecordDaoUtil::toSingleOptionalErrorRecord);
  }

  /**
   * Saves {@link ErrorRecord} to the db using {@link QueryExecutor}
   *
   * @param queryExecutor query executor
   * @param errorRecord   error record
   * @return future with updated ErrorRecord
   */
  public static Future<ErrorRecord> save(QueryExecutor queryExecutor, ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = toDatabaseErrorRecord(errorRecord);
    return queryExecutor.execute(dsl -> dsl.insertInto(ERROR_RECORDS_LB)
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
    return new ErrorRecord()
      .withId(row.getUUID(ERROR_RECORDS_LB.ID.getName()).toString())
      .withContent(row.getString(ERROR_RECORDS_LB.CONTENT.getName()))
      .withDescription(row.getString(ERROR_RECORDS_LB.DESCRIPTION.getName()));
  }

  /**
   * Convert database query result {@link Row} to {@link ErrorRecord}
   *
   * @param row query result row
   * @return ErrorRecord
   */
  public static ErrorRecord toJoinedErrorRecord(Row row) {
    ErrorRecord errorRecord = new ErrorRecord();
    UUID id = row.getUUID(ID);
    if (Objects.nonNull(id)) {
      errorRecord.withId(id.toString());
    }
    return errorRecord
      .withContent(row.getString(ERROR_RECORD_CONTENT))
      .withDescription(row.getString(DESCRIPTION));
  }

  /**
   * Convert database query result {@link Row} to {@link ErrorRecord}
   *
   * @param dbRecord query result record
   * @return ErrorRecord
   */
  public static ErrorRecord toJoinedErrorRecord(Record dbRecord) {
    ErrorRecord errorRecord = new ErrorRecord();
    UUID id = dbRecord.get(ERROR_RECORDS_LB.ID);
    if (Objects.nonNull(id)) {
      errorRecord.withId(id.toString());
    }
    return errorRecord
      .withContent(dbRecord.get(ERROR_RECORD_CONTENT, String.class))
      .withDescription(dbRecord.get(ERROR_RECORDS_LB.DESCRIPTION));
  }

  /**
   * Convert database query result {@link RowSet} to {@link Optional} {@link ErrorRecord}
   *
   * @param rowSet query result row set
   * @return optional ErrorRecord
   */
  public static Optional<ErrorRecord> toSingleOptionalErrorRecord(RowSet<Row> rowSet) {
    return rowSet.size() == 0 ? Optional.empty() : Optional.of(toErrorRecord(rowSet.iterator().next()));
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
      if (errorRecord.getContent() instanceof String contentString) {
        dbRecord.setContent(contentString);
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
