package org.folio.dao.util;

import static org.folio.rest.jooq.Tables.ERROR_RECORDS_LB;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import io.vertx.reactivex.sqlclient.Tuple;
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
import org.jooq.DSLContext;
import org.jooq.InsertResultStep;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

/**
 * Utility class for managing {@link ErrorRecord}
 */
public final class ErrorRecordDaoUtil {

  private static final String ID = "id";
  private static final String DESCRIPTION = "description";

  public static final String ERROR_RECORD_CONTENT = "error_record_content";

  private static final DSLContext dslContext = DSL.using(SQLDialect.POSTGRES, new Settings()
    .withParamType(ParamType.NAMED)
    .withRenderNamedParamPrefix("$"));

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
   * Searches for {@link ErrorRecord} by id using {@link io.vertx.reactivex.sqlclient.SqlConnection}
   *
   * @param sqlConnection sql connection
   * @param id            id
   * @return future with optional ErrorRecord
   */
  public static Future<Optional<ErrorRecord>> findById(io.vertx.reactivex.sqlclient.SqlConnection sqlConnection, String id) {
    SelectConditionStep<ErrorRecordsLbRecord> query = dslContext.selectFrom(ERROR_RECORDS_LB)
      .where(ERROR_RECORDS_LB.ID.eq(UUID.fromString(id)));
    return sqlConnection.preparedQuery(query.getSQL())
      .execute(Tuple.from(query.getBindValues()))
      .map(io.vertx.reactivex.sqlclient.RowSet::iterator)
      .map(iterator -> iterator.hasNext() ? Optional.of(toErrorRecord(iterator.next().getDelegate())) : Optional.empty());
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
   * Saves {@link ErrorRecord} to the db
   *
   * @param errorRecord {@link ErrorRecord} to save
   * @return future with saved {@link ErrorRecord}
   */
  public static Future<ErrorRecord> save(io.vertx.reactivex.sqlclient.SqlConnection connection, ErrorRecord errorRecord) {
    ErrorRecordsLbRecord dbRecord = toDatabaseErrorRecord(errorRecord);
    InsertResultStep<ErrorRecordsLbRecord> query = dslContext.insertInto(ERROR_RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning();

    return connection.preparedQuery(query.getSQL())
      .execute(Tuple.from(query.getBindValues()))
      .map(io.vertx.reactivex.sqlclient.RowSet::getDelegate)
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
    UUID id = dbRecord.get(org.folio.rest.jooq.tables.ErrorRecordsLb.ERROR_RECORDS_LB.ID);
    if (Objects.nonNull(id)) {
      errorRecord.withId(id.toString());
    }
    return errorRecord
      .withContent(dbRecord.get(ERROR_RECORD_CONTENT, String.class))
      .withDescription(dbRecord.get(org.folio.rest.jooq.tables.ErrorRecordsLb.ERROR_RECORDS_LB.DESCRIPTION));
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
