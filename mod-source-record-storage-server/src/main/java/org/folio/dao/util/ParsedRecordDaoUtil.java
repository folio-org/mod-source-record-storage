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

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;

/**
 * Utility class for managing {@link ParsedRecord}
 */
public final class ParsedRecordDaoUtil {

  private static final String LEADER = "leader";
  private static final String CONTENT = "content";

  public static final String ID = "id";

  private ParsedRecordDaoUtil() { }

  /**
   * Searches for {@link ParsedRecord} by id using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param id            id
   * @param recordType    record type to find
   * @return future with optional ParsedRecord
   */
  public static Future<Optional<ParsedRecord>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id, RecordType recordType) {
    String tableName = recordType.getTableName();
    Field<UUID> idField = field(name(ID), UUID.class);
    Field<String> contentField = field(name(CONTENT), String.class);
    return queryExecutor.findOneRow(dsl -> dsl.select(idField, contentField)
      .from(table(name(tableName)))
      .where(idField.eq(UUID.fromString(id))))
        .map(ParsedRecordDaoUtil::toOptionalParsedRecord);
  }

  /**
   * Saves {@link ParsedRecord} to the db table defined by {@link RecordType} using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param parsedRecord  parsed record
   * @param recordType    record type to save
   * @return future with updated ParsedRecord
   */
  public static Future<ParsedRecord> save(ReactiveClassicGenericQueryExecutor queryExecutor, ParsedRecord parsedRecord, RecordType recordType) {
    String tableName = recordType.getTableName();
    Field<UUID> idField = field(name(ID), UUID.class);
    Field<String> contentField = field(name(CONTENT), String.class);
    UUID id = UUID.fromString(parsedRecord.getId());
    String content = (String) normalizeContent(parsedRecord).getContent();
    return queryExecutor.executeAny(dsl -> dsl.insertInto(table(name(tableName)))
      .set(idField, id)
      .set(contentField, content)
      .onConflict(idField)
        .doUpdate()
          .set(contentField, content)
          .returning())
        .map(res -> parsedRecord);
  }

  /**
   * Updates {@link ParsedRecord} to the db table defined by {@link RecordType} using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param parsedRecord  parsed record to update
   * @param recordType    record type to update
   * @return future of updated ParsedRecord
   */
  public static Future<ParsedRecord> update(ReactiveClassicGenericQueryExecutor queryExecutor, ParsedRecord parsedRecord, RecordType recordType) {
    String tableName = recordType.getTableName();
    Field<UUID> idField = field(name(ID), UUID.class);
    Field<String> contentField = field(name(CONTENT), String.class);
    UUID id = UUID.fromString(parsedRecord.getId());
    String content = (String) normalizeContent(parsedRecord).getContent();
    return queryExecutor.executeAny(dsl -> dsl.update(table(name(tableName)))
      .set(contentField, content)
      .where(idField.eq(id)))
        .map(update -> {
          if (update.rowCount() > 0) {
            return parsedRecord;
          }
          throw new NotFoundException(String.format("ParsedRecord with id '%s' was not found", parsedRecord.getId()));
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
    return parsedRecord
      .withContent(row.getString(CONTENT));
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
   * Ensure normalize content of {@link ParsedRecord} is type {@link String}
   * 
   * @param parsedRecord parsed record
   * @return parsed record with normalized content
   */
  public static ParsedRecord normalizeContent(ParsedRecord parsedRecord) {
    String content;
    if (parsedRecord.getContent() instanceof String) {
      content = (String) parsedRecord.getContent();
    } else {
      content = JsonObject.mapFrom(parsedRecord.getContent()).encode();
    }
    return parsedRecord.withContent(content);
  }

  /**
   * Extract MARC Leader status 05 from {@link ParsedRecord} content.
   * 
   * @param parsedRecord parsed record
   * @return MARC Leader status 05
   */
  public static String getLeaderStatus(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord)) {
      JsonObject content;
      if (parsedRecord.getContent() instanceof String) {
        content = new JsonObject((String) parsedRecord.getContent());
      } else {
        content = JsonObject.mapFrom(parsedRecord.getContent());
      }
      String leader = content.getString(LEADER);
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

}