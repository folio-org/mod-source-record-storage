package org.folio.dao.util;

import static org.folio.rest.jooq.Tables.RAW_RECORDS_LB;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.RawRecordsLb;
import org.folio.rest.jooq.tables.records.RawRecordsLbRecord;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.jooq.Record;

/**
 * Utility class for managing {@link RawRecord}
 */
public final class RawRecordDaoUtil {

  private static final String ID = "id";

  public static final String RAW_RECORD_CONTENT = "raw_record_content";

  private RawRecordDaoUtil() { }

  /**
   * Searches for {@link RawRecord} by id using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param queryExecutor query executor
   * @param id            id
   * @return future with optional RawRecord
   */
  public static Future<Optional<RawRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(RAW_RECORDS_LB)
      .where(RAW_RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(RawRecordDaoUtil::toOptionalRawRecord);
  }

  /**
   * Saves {@link RawRecord} to the db using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param queryExecutor query executor
   * @param rawRecord     raw record
   * @return future with updated RawRecord
   */
  public static Future<RawRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor, RawRecord rawRecord) {
    RawRecordsLbRecord dbRecord = toDatabaseRawRecord(rawRecord);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(RAW_RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(RawRecordDaoUtil::toSingleRawRecord);
  }

  /**
   * Convert database query result {@link Row} to {@link RawRecord}
   *
   * @param row query result row
   * @return RawRecord
   */
  public static RawRecord toRawRecord(Row row) {
    RawRecordsLb pojo = RowMappers.getRawRecordsLbMapper().apply(row);
    return new RawRecord()
      .withId(pojo.getId().toString())
      .withContent(pojo.getContent());
  }

  /**
   * Convert database query result {@link Row} to {@link RawRecord}
   *
   * @param row query result row
   * @return RawRecord
   */
  public static RawRecord toJoinedRawRecord(Row row) {
    RawRecord rawRecord = new RawRecord();
    UUID id = row.getUUID(ID);
    if (Objects.nonNull(id)) {
      rawRecord.withId(id.toString());
    }
    return rawRecord
      .withContent(row.getString(RAW_RECORD_CONTENT));
  }

  /**
   * Convert database query result {@link Record} to {@link RawRecord}
   *
   * @param dbRecord query result record
   * @return RawRecord
   */
  public static RawRecord toJoinedRawRecord(Record dbRecord) {
    RawRecord rawRecord = new RawRecord();
    UUID id = dbRecord.get(org.folio.rest.jooq.tables.RawRecordsLb.RAW_RECORDS_LB.ID);
    if (Objects.nonNull(id)) {
      rawRecord.withId(id.toString());
    }
    return rawRecord
      .withContent(dbRecord.get(RAW_RECORD_CONTENT, String.class));
  }

  /**
   * Convert database query result {@link Row} to {@link Optional} {@link RawRecord}
   *
   * @param row query result row
   * @return optional RawRecord
   */
  public static Optional<RawRecord> toOptionalRawRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toRawRecord(row)) : Optional.empty();
  }

  /**
   * Convert {@link RawRecord} to database record {@link RawRecordsLbRecord}
   *
   * @param rawRecord raw record
   * @return RawRecordsLbRecord
   */
  public static RawRecordsLbRecord toDatabaseRawRecord(RawRecord rawRecord) {
    RawRecordsLbRecord dbRecord = new RawRecordsLbRecord();
    if (StringUtils.isNotEmpty(rawRecord.getId())) {
      dbRecord.setId(UUID.fromString(rawRecord.getId()));
    }
    dbRecord.setContent(rawRecord.getContent());
    return dbRecord;
  }

  private static RawRecord toSingleRawRecord(RowSet<Row> rows) {
    return toRawRecord(rows.iterator().next());
  }

}
