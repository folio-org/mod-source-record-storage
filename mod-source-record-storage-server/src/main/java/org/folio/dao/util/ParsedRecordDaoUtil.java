package org.folio.dao.util;

import static java.lang.String.format;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jooq.tables.records.EdifactRecordsLbRecord;
import org.folio.rest.jooq.tables.records.MarcRecordsLbRecord;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.impl.SQLDataType;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.postgres.JSONBToJsonObjectConverter;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;

/**
 * Utility class for managing {@link ParsedRecord}
 */
public final class ParsedRecordDaoUtil {

  private static final String ID = "id";
  private static final String CONTENT = "content";
  private static final String LEADER = "leader";

  private static final Field<UUID> ID_FIELD = field(name(ID), UUID.class);
  private static final Field<JsonObject> CONTENT_FIELD = field(name(CONTENT), SQLDataType.JSONB.asConvertedDataType(new JSONBToJsonObjectConverter()));

  public static final String PARSED_RECORD_NOT_FOUND_TEMPLATE = "Parsed Record with id '%s' was not found";

  public static final String PARSED_RECORD_CONTENT = "parsed_record_content";

  private ParsedRecordDaoUtil() { }

  /**
   * Searches for {@link ParsedRecord} by id using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param queryExecutor query executor
   * @param id            id
   * @param recordType    record type to find
   * @return future with optional ParsedRecord
   */
  public static Future<Optional<ParsedRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor,
      String id, RecordType recordType) {
    return queryExecutor.findOneRow(dsl -> dsl.select(ID_FIELD, CONTENT_FIELD)
      .from(table(name(recordType.getTableName())))
      .where(ID_FIELD.eq(UUID.fromString(id))))
      .map(ParsedRecordDaoUtil::toOptionalParsedRecord);
  }

  /**
   * Saves {@link ParsedRecord} to the db table defined by {@link RecordType} using
   * {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param queryExecutor query executor
   * @param parsedRecord  parsed record
   * @param recordType    record type to save
   * @return future with updated ParsedRecord
   */
  public static Future<ParsedRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor,
      ParsedRecord parsedRecord, RecordType recordType) {
    UUID id = UUID.fromString(parsedRecord.getId());
    JsonObject content = normalize(parsedRecord.getContent());
    return queryExecutor.executeAny(dsl -> dsl.insertInto(table(name(recordType.getTableName())))
      .set(ID_FIELD, id)
      .set(CONTENT_FIELD, content)
      .onConflict(ID_FIELD)
      .doUpdate()
      .set(CONTENT_FIELD, content)
      .returning())
      .map(res -> parsedRecord
        .withContent(content.getMap()));
  }

  /**
   * Updates {@link ParsedRecord} to the db table defined by {@link RecordType} using
   * {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param queryExecutor query executor
   * @param parsedRecord  parsed record to update
   * @param recordType    record type to update
   * @return future of updated ParsedRecord
   */
  public static Future<ParsedRecord> update(ReactiveClassicGenericQueryExecutor queryExecutor,
      ParsedRecord parsedRecord, RecordType recordType) {
    UUID id = UUID.fromString(parsedRecord.getId());
    JsonObject content = normalize(parsedRecord.getContent());
    return queryExecutor.executeAny(dsl -> dsl.update(table(name(recordType.getTableName())))
      .set(CONTENT_FIELD, content)
      .where(ID_FIELD.eq(id)))
      .map(update -> {
        if (update.rowCount() > 0) {
          return parsedRecord
            .withContent(content.getMap());
        }
        String message = format(PARSED_RECORD_NOT_FOUND_TEMPLATE, parsedRecord.getId());
        throw new NotFoundException(message);
      });
  }

  /**
   * Convert database query result {@link Row} to {@link ParsedRecord}
   *
   * @param row query result row
   * @return ParsedRecord
   */
  public static ParsedRecord toParsedRecord(Row row) {
    ParsedRecord parsedRecord = new ParsedRecord();
    UUID id = row.getUUID(ID);
    if (Objects.nonNull(id)) {
      parsedRecord.withId(id.toString());
    }
    Object content = row.getValue(CONTENT);
    if (Objects.nonNull(content)) {
      parsedRecord.withContent(normalize(content).getMap());
    }
    return parsedRecord;
  }

  /**
   * Convert database query result {@link Row} to {@link ParsedRecord}
   *
   * @param row query result row
   * @return ParsedRecord
   */
  public static ParsedRecord toJoinedParsedRecord(Row row) {
    ParsedRecord parsedRecord = new ParsedRecord();
    UUID id = row.getUUID(ID);
    if (Objects.nonNull(id)) {
      parsedRecord.withId(id.toString());
    }
    Object content = row.getValue(PARSED_RECORD_CONTENT);
    if (Objects.nonNull(content)) {
      parsedRecord.withContent(normalize(content).getMap());
    }
    return parsedRecord;
  }

  /**
   * Convert database query result {@link Row} to {@link Optional} {@link ErrorRecord}
   *
   * @param row query result row
   * @return optional ParsedRecord
   */
  public static Optional<ParsedRecord> toOptionalParsedRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toParsedRecord(row)) : Optional.empty();
  }

  /**
   * Normalize parsed record content content of {@link ParsedRecord} is type {@link String}
   *
   * @param parsedRecord parsed record
   * @return parsed record normalized content
   */
  public static String normalizeContent(ParsedRecord parsedRecord) {
    return normalize(parsedRecord.getContent()).encode();
  }

  /**
   * Extract MARC Leader status 05 from {@link ParsedRecord} content.
   *
   * @param parsedRecord parsed record
   * @return MARC Leader status 05
   */
  public static String getLeaderStatus(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord)) {
      JsonObject marcJson = normalize(parsedRecord.getContent());
      String leader = marcJson.getString(LEADER);
      if (Objects.nonNull(leader) && leader.length() > 5) {
        return String.valueOf(leader.charAt(5));
      }
    }
    return null;
  }

  /**
   * Convert {@link ParsedRecord} to database record {@link MarcRecordsLbRecord}
   *
   * @param parsedRecord parsed record
   * @return MarcRecordsLbRecord
   */
  public static MarcRecordsLbRecord toDatabaseMarcRecord(ParsedRecord parsedRecord) {
    MarcRecordsLbRecord dbRecord = new MarcRecordsLbRecord();
    if (StringUtils.isNotEmpty(parsedRecord.getId())) {
      dbRecord.setId(UUID.fromString(parsedRecord.getId()));
    }
    JsonObject jsonContent = normalize(parsedRecord.getContent());
    dbRecord.setContent(JSONB.valueOf(jsonContent.encode()));
    return dbRecord;
  }

  /**
   * Convert {@link ParsedRecord} to database record {@link EdifactRecordsLbRecord}
   *
   * @param parsedRecord parsed record
   * @return EdifactRecordsLbRecord
   */
  public static EdifactRecordsLbRecord toDatabaseEdifactRecord(ParsedRecord parsedRecord) {
    EdifactRecordsLbRecord dbRecord = new EdifactRecordsLbRecord();
    if (StringUtils.isNotEmpty(parsedRecord.getId())) {
      dbRecord.setId(UUID.fromString(parsedRecord.getId()));
    }
    JsonObject jsonContent = normalize(parsedRecord.getContent());
    dbRecord.setContent(JSONB.valueOf(jsonContent.encode()));
    return dbRecord;
  }

  /**
   * Convinience method to get {@link RecordType} from {@link Record}
   *
   * @param record record
   * @return record type defaulting to MARC
   */
  public static RecordType toRecordType(Record record) {
    if (Objects.nonNull(record.getRecordType())) {
      return RecordType.valueOf(record.getRecordType().toString());
    }
    return RecordType.MARC_BIB;
  }

  private static JsonObject normalize(Object content) {
    return (content instanceof String)
      ? new JsonObject((String) content)
      : JsonObject.mapFrom(content);
  }

}
