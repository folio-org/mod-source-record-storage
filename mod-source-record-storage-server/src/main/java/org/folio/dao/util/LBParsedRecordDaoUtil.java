package org.folio.dao.util;

import static org.folio.rest.jooq.Tables.MARC_RECORDS_LB;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.MarcRecordsLb;
import org.folio.rest.jooq.tables.records.MarcRecordsLbRecord;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class LBParsedRecordDaoUtil {

  private LBParsedRecordDaoUtil() { }

  public static Future<Optional<ParsedRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(MARC_RECORDS_LB)
      .where(MARC_RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(LBParsedRecordDaoUtil::toOptionalParsedRecord);
  }

  public static Future<ParsedRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor, ParsedRecord parsedRecord) {
    MarcRecordsLbRecord dbRecord = toDatabaseParsedRecord(parsedRecord);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(MARC_RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(LBParsedRecordDaoUtil::toSingleParsedRecord);
  }

  public static Future<ParsedRecord> update(ReactiveClassicGenericQueryExecutor queryExecutor, ParsedRecord parsedRecord) {
    MarcRecordsLbRecord dbRecord = toDatabaseParsedRecord(parsedRecord);
    return queryExecutor.executeAny(dsl -> dsl.update(MARC_RECORDS_LB)
      .set(dbRecord)
      .where(MARC_RECORDS_LB.ID.eq(UUID.fromString(parsedRecord.getId())))
      .returning())
        .map(LBParsedRecordDaoUtil::toSingleOptionalParsedRecord)
        .map(optionalParsedRecord -> {
          if (optionalParsedRecord.isPresent()) {
            return optionalParsedRecord.get();
          }
          throw new NotFoundException(String.format("ParsedRecord with id '%s' was not found", parsedRecord.getId()));
        });
  }

  public static ParsedRecord toParsedRecord(Row row) {
    MarcRecordsLb pojo = RowMappers.getMarcRecordsLbMapper().apply(row);
    return new ParsedRecord()
      .withId(pojo.getId().toString())
      .withContent(pojo.getContent());
  }

  public static Optional<ParsedRecord> toOptionalParsedRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toParsedRecord(row)) : Optional.empty();
  }

  public static MarcRecordsLbRecord toDatabaseParsedRecord(ParsedRecord parsedRecord) {
    MarcRecordsLbRecord dbRecord = new MarcRecordsLbRecord();
    if (StringUtils.isNotEmpty(parsedRecord.getId())) {
      dbRecord.setId(UUID.fromString(parsedRecord.getId()));
    }
    if (Objects.nonNull(parsedRecord.getContent())) {
      dbRecord.setContent((String) normalizeContent(parsedRecord).getContent());
    }
    return dbRecord;
  }

  public static ParsedRecord normalizeContent(ParsedRecord parsedRecord) {
    String content;
      if (parsedRecord.getContent() instanceof String) {
        content = (String) parsedRecord.getContent();
      } else {
        content = JsonObject.mapFrom(parsedRecord.getContent()).encode();
      }
    return parsedRecord.withContent(content);
  }

  private static ParsedRecord toSingleParsedRecord(RowSet<Row> rows) {
    return toParsedRecord(rows.iterator().next());
  }

  private static Optional<ParsedRecord> toSingleOptionalParsedRecord(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toParsedRecord(rows.iterator().next())) : Optional.empty();
  }

}