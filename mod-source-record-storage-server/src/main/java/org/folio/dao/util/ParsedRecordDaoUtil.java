package org.folio.dao.util;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Field;
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

  public static final String PARSED_RECORD_CONTENT = "parsed_record_content";

  private ParsedRecordDaoUtil() {
  }

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
    String tableName = recordType.getTableName();
    Field<UUID> idField = field(name(ID), UUID.class);
    Field<JsonObject> contentField = field(name(CONTENT), SQLDataType.JSONB.asConvertedDataType(new JSONBToJsonObjectConverter()));
    return queryExecutor.findOneRow(dsl -> dsl.select(idField, contentField)
      .from(table(name(tableName)))
      .where(idField.eq(UUID.fromString(id))))
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
    String tableName = recordType.getTableName();
    Field<UUID> idField = field(name(ID), UUID.class);
    Field<JsonObject> contentField = field(name(CONTENT), SQLDataType.JSONB.asConvertedDataType(new JSONBToJsonObjectConverter()));
    UUID id = UUID.fromString(parsedRecord.getId());
    JsonObject content = normalize(parsedRecord.getContent());
    return queryExecutor.executeAny(dsl -> dsl.insertInto(table(name(tableName)))
      .set(idField, id)
      .set(contentField, content)
      .onConflict(idField)
      .doUpdate()
      .set(contentField, content)
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
    String tableName = recordType.getTableName();
    Field<UUID> idField = field(name(ID), UUID.class);
    Field<JsonObject> contentField = field(name(CONTENT), SQLDataType.JSONB.asConvertedDataType(new JSONBToJsonObjectConverter()));
    UUID id = UUID.fromString(parsedRecord.getId());
    JsonObject content = normalize(parsedRecord.getContent());
    return queryExecutor.executeAny(dsl -> dsl.update(table(name(tableName)))
      .set(contentField, content)
      .where(idField.eq(id)))
      .map(update -> {
        if (update.rowCount() > 0) {
          return parsedRecord
            .withContent(content.getMap());
        }
        String message = String.format("ParsedRecord with id '%s' was not found", parsedRecord.getId());
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
   * Convinience method to get {@link RecordType} from {@link Record}
   *
   * @param record record
   * @return record type defaulting to MARC
   */
  public static RecordType toRecordType(Record record) {
    if (Objects.nonNull(record.getRecordType())) {
      return RecordType.valueOf(record.getRecordType().toString());
    }
    return RecordType.MARC;
  }

  private static JsonObject normalize(Object content) {
    return (content instanceof String)
      ? new JsonObject((String) content)
      : JsonObject.mapFrom(content);
  }

}
