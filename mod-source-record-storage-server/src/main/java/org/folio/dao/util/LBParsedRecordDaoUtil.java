package org.folio.dao.util;

import static org.folio.rest.jooq.Tables.MARC_RECORDS_LB;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.MarcRecordsLb;
import org.folio.rest.jooq.tables.records.MarcRecordsLbRecord;
import org.jooq.Condition;
import org.jooq.InsertSetStep;
import org.jooq.InsertValuesStepN;
import org.jooq.OrderField;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class LBParsedRecordDaoUtil {

  private LBParsedRecordDaoUtil() { }

  public static Future<List<ParsedRecord>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition,
      Collection<OrderField<?>> orderFields, int offset, int limit) {
    return queryExecutor.executeAny(dsl -> dsl.selectFrom(MARC_RECORDS_LB)
      .where(condition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit))
        .map(LBParsedRecordDaoUtil::toParsedRecords);
  }

  public static Future<Optional<ParsedRecord>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(MARC_RECORDS_LB)
      .where(condition))
        .map(LBParsedRecordDaoUtil::toOptionalParsedRecord);
  }

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
        .map(LBParsedRecordDaoUtil::toParsedRecord);
  }

  public static Future<List<ParsedRecord>> save(ReactiveClassicGenericQueryExecutor queryExecutor, List<ParsedRecord> parsedRecords) {
    return queryExecutor.executeAny(dsl -> {
      InsertSetStep<MarcRecordsLbRecord> insertSetStep = dsl.insertInto(MARC_RECORDS_LB);
      InsertValuesStepN<MarcRecordsLbRecord> insertValuesStepN = null;
      for (ParsedRecord parsedRecord : parsedRecords) {
          insertValuesStepN = insertSetStep.values(toDatabaseParsedRecord(parsedRecord).intoArray());
      }
      return insertValuesStepN;
    }).map(LBParsedRecordDaoUtil::toParsedRecords);
  }

  public static Future<ParsedRecord> update(ReactiveClassicGenericQueryExecutor queryExecutor, ParsedRecord parsedRecord) {
    MarcRecordsLbRecord dbRecord = toDatabaseParsedRecord(parsedRecord);
    return queryExecutor.executeAny(dsl -> dsl.update(MARC_RECORDS_LB)
      .set(dbRecord)
      .where(MARC_RECORDS_LB.ID.eq(UUID.fromString(parsedRecord.getId())))
      .returning())
        .map(LBParsedRecordDaoUtil::toOptionalParsedRecord)
        .map(optionalParsedRecord -> {
          if (optionalParsedRecord.isPresent()) {
            return optionalParsedRecord.get();
          }
          throw new NotFoundException(String.format("ParsedRecord with id '%s' was not found", parsedRecord.getId()));
        });
  }

  public static Future<Boolean> delete(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(MARC_RECORDS_LB)
      .where(MARC_RECORDS_LB.ID.eq(UUID.fromString(id))))
      .map(res -> res == 1);
  }

  public static Future<Integer> deleteAll(ReactiveClassicGenericQueryExecutor queryExecutor) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(MARC_RECORDS_LB));
  }

  public static ParsedRecord toParsedRecord(Row row) {
    MarcRecordsLb pojo = RowMappers.getMarcRecordsLbMapper().apply(row);
    return new ParsedRecord()
      .withId(pojo.getId().toString())
      .withContent(pojo.getContent());
  }

  public static MarcRecordsLbRecord toDatabaseParsedRecord(ParsedRecord parsedRecord) {
    MarcRecordsLbRecord dbRecord = new MarcRecordsLbRecord();
    if (StringUtils.isNotEmpty(parsedRecord.getId())) {
      dbRecord.setId(UUID.fromString(parsedRecord.getId()));
    }
    if (Objects.nonNull(parsedRecord.getContent())) {
      if (parsedRecord.getContent() instanceof LinkedHashMap) {
        dbRecord.setContent(JsonObject.mapFrom(parsedRecord.getContent()).encode());
      } else {
        dbRecord.setContent((String) parsedRecord.getContent());
      }
    }
    return dbRecord;
  }

  private static ParsedRecord toParsedRecord(RowSet<Row> rows) {
    return toParsedRecord(rows.iterator().next());
  }

  private static List<ParsedRecord> toParsedRecords(RowSet<Row> rows) {
    return StreamSupport.stream(rows.spliterator(), false)
      .map(LBParsedRecordDaoUtil::toParsedRecord)
      .collect(Collectors.toList());
  }

  private static Optional<ParsedRecord> toOptionalParsedRecord(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toParsedRecord(rows.iterator().next())) : Optional.empty();
  }

  private static Optional<ParsedRecord> toOptionalParsedRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toParsedRecord(row)) : Optional.empty();
  }

}