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

public class LBRawRecordDaoUtil {

  private LBRawRecordDaoUtil() { }

  public static Future<Optional<RawRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(RAW_RECORDS_LB)
      .where(RAW_RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(LBRawRecordDaoUtil::toOptionalRawRecord);
  }

  public static Future<RawRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor, RawRecord rawRecord) {
    RawRecordsLbRecord dbRecord = toDatabaseRawRecord(rawRecord);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(RAW_RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(LBRawRecordDaoUtil::toSingleRawRecord);
  }

  public static RawRecord toRawRecord(Row row) {
    RawRecordsLb pojo = RowMappers.getRawRecordsLbMapper().apply(row);
    return new RawRecord()
      .withId(pojo.getId().toString())
      .withContent(pojo.getContent());
  }

  public static Optional<RawRecord> toOptionalRawRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toRawRecord(row)) : Optional.empty();
  }

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