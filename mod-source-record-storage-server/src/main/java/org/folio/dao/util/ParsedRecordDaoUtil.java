package org.folio.dao.util;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.CustomReactiveQueryExecutor;
import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.github.jklingsporn.vertx.jooq.shared.postgres.JSONBToJsonObjectConverter;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jooq.routines.UpsertMarcRecord;
import org.folio.rest.jooq.tables.records.EdifactRecordsLbRecord;
import org.folio.rest.jooq.tables.records.MarcIndexersRecord;
import org.folio.rest.jooq.tables.records.MarcRecordsLbRecord;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.rest.jooq.Tables.MARC_INDEXERS;
import static org.folio.rest.jooq.Tables.MARC_RECORDS_TRACKING;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.table;

/**
 * Utility class for managing {@link ParsedRecord}
 */
public final class ParsedRecordDaoUtil {

  private static final String ID = "id";
  private static final String CONTENT = "content";
  private static final String LEADER = "leader";
  private static final int LEADER_STATUS_SUBFIELD_POSITION = 5;

  public static final Field<UUID> ID_FIELD = field(name(ID), UUID.class);
  public static final Field<JsonObject> CONTENT_FIELD = field(name(CONTENT), SQLDataType.JSONB.asConvertedDataType(new JSONBToJsonObjectConverter()));
  private static final Field<UUID> MARC_ID_FIELD = field(name("marc_id"), UUID.class);
  public static final String PARSED_RECORD_NOT_FOUND_TEMPLATE = "Parsed Record with id '%s' was not found";
  public static final String PARSED_RECORD_CONTENT = "parsed_record_content";
  private static final MarcIndexersUpdatedIds UPDATE_MARC_INDEXERS_TEMP_TABLE = new MarcIndexersUpdatedIds();

  public static class MarcIndexersUpdatedIds extends TableImpl<Record1<UUID>> {
    public final TableField<Record1<UUID>, UUID> MARC_ID = createField(DSL.name("marc_id"), SQLDataType.UUID);

    private MarcIndexersUpdatedIds() {
      super(DSL.name("marc_indexers_updated_ids"));
    }
  }

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
  public static Future<ParsedRecord> save(CustomReactiveQueryExecutor queryExecutor,
      ParsedRecord parsedRecord, RecordType recordType) {
    UUID id = UUID.fromString(parsedRecord.getId());
    JsonObject content = normalize(parsedRecord.getContent());
    UpsertMarcRecord upsertMarcRecord = new UpsertMarcRecord();
    upsertMarcRecord.setRecordId(id);
    upsertMarcRecord.setContent(content);
    return queryExecutor.executeAny(dsl -> dsl.select(upsertMarcRecord.asField()))
      .compose(res -> {
        Row row = res.iterator().next();
        if(row == null) {
          return Future.failedFuture(
            String.format("save:: a version was not returned upon upsert of a marc record marRecordId=%s", id));
        }
        Integer version = row.getInteger(0);
        return updateMarcIndexersTableAsync(queryExecutor, recordType, id, content, version)
            .compose(ar -> Future.succeededFuture(res));
        }
      )
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
  public static Future<ParsedRecord> update(CustomReactiveQueryExecutor queryExecutor,
      ParsedRecord parsedRecord, RecordType recordType) {
    UUID id = UUID.fromString(parsedRecord.getId());
    JsonObject content = normalize(parsedRecord.getContent());
    UpsertMarcRecord upsertMarcRecord = new UpsertMarcRecord();
    upsertMarcRecord.setRecordId(id);
    upsertMarcRecord.setContent(content);
    return queryExecutor.executeAny(dsl -> dsl.select(upsertMarcRecord.asField()))
      .compose(res -> {
          Row row = res.iterator().next();
          if (row == null) {
            return Future.failedFuture(
              String.format("update:: a version was not returned upon upsert of a marc record marRecordId=%s", id));
          }
          Integer version = row.getInteger(0);
          return updateMarcIndexersTableAsync(queryExecutor, recordType, id, content, version)
            .compose(ar -> Future.succeededFuture(res));
        }
      )
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
   * Synchronously updates the MARC indexers table based on the parsed records provided. This method first validates
   * if the provided record type matches 'MARC_BIB'. If not, it terminates without performing any operation.
   * Otherwise, it seeds the temporary table with MARC record IDs, retrieves their versions and inserts new indexers
   * based on the parsed records data.
   *
   * @param dsl The DSLContext instance used to create and execute SQL queries.
   * @param recordType The type of the record to be updated.
   * @param parsedRecords A map containing the UUIDs of the records as keys and the parsed JSONB content as values.
   *
   * @throws DataAccessException if the execution of any of the SQL queries fails.
   */
  public static void updateMarcIndexersTableSync(DSLContext dsl, RecordType recordType, Map<UUID, JSONB> parsedRecords) throws IOException {
    if (!recordType.getTableName().equals(RecordType.MARC_BIB.getTableName())) {
      return;
    }

    // Seed marc records identifiers before getting their versions
    dsl.createTemporaryTableIfNotExists(UPDATE_MARC_INDEXERS_TEMP_TABLE)
      .column(UPDATE_MARC_INDEXERS_TEMP_TABLE.MARC_ID)
      .onCommitDrop()
      .execute();

    List<Record1<UUID>> tempIds = parsedRecords.keySet().stream().map(k -> {
      Record1<UUID> uuidRecord1 = dsl.newRecord(UPDATE_MARC_INDEXERS_TEMP_TABLE.MARC_ID);
      uuidRecord1.set(UPDATE_MARC_INDEXERS_TEMP_TABLE.MARC_ID, k);
      return uuidRecord1;
    }).collect(Collectors.toList());

    dsl.loadInto(UPDATE_MARC_INDEXERS_TEMP_TABLE)
      .batchAfter(250)
      .onErrorAbort()
      .loadRecords(tempIds)
      .fieldsCorresponding()
      .execute();

    // Get marc records versions
    Table<Record1<UUID>> subQuery = select(UPDATE_MARC_INDEXERS_TEMP_TABLE.MARC_ID)
      .from(UPDATE_MARC_INDEXERS_TEMP_TABLE).asTable("subquery");
    var query = dsl
      .select(MARC_RECORDS_TRACKING.MARC_ID, MARC_RECORDS_TRACKING.VERSION)
      .from(MARC_RECORDS_TRACKING)
      .where(MARC_RECORDS_TRACKING.MARC_ID.in(select(MARC_ID_FIELD).from(subQuery)));
    var marcIndexersVersions = query.fetch();

    // Insert indexers
    List<org.jooq.Record> indexers = marcIndexersVersions.stream()
      .map(record -> {
        JSONB jsonb = parsedRecords.get(record.value1());
        if (jsonb != null) {
          return createMarcIndexerRecords(dsl, record.value1(), new JsonObject(jsonb.data()), record.value2());
        }
        return Collections.<org.jooq.Record>emptyList();
      })
      .flatMap(Collection::stream)
      .collect(Collectors.toList());

    dsl.loadInto(MARC_INDEXERS)
      .batchAfter(250)
      .onErrorAbort()
      .loadRecords(indexers)
      .fieldsCorresponding()
      .execute();
  }

  /**
   * Updates the MARC indexers table asynchronously. This method gets the MARC versions based on the provided object id
   * and if the record is marked as dirty, it creates and inserts new MARC indexer records into the MARC indexers table.
   * If the record type does not match 'MARC_BIB', or if the record is not marked as dirty, the method immediately
   * completes with a 'false' result.
   *
   * @param queryExecutor The executor to run the database queries.
   * @param recordType The type of the record to be updated.
   * @param objectId The unique identifier of the object to be updated.
   * @param content The content to be used for creating new MARC indexer records.
   *
   * @return A Future containing 'true' if the MARC indexer records are created and inserted successfully, 'false'
   *         otherwise. If the MARC record with the provided id cannot be found or there are multiple such records,
   *         a RuntimeException is thrown.
   *
   * @throws RuntimeException if the MARC record with the provided id cannot be found or there are multiple such records.
   */
  public static Future<Boolean> updateMarcIndexersTableAsync(CustomReactiveQueryExecutor queryExecutor,
                                                             RecordType recordType,
                                                             UUID objectId,
                                                             JsonObject content,
                                                             Integer version) {
    if (!recordType.getTableName().equals(RecordType.MARC_BIB.getTableName())) {
      return Future.succeededFuture(false);
    }

    DSLContext dslContext = DSL.using(queryExecutor.configuration());
    Collection<MarcIndexersRecord> marcIndexerRecords =
      createMarcIndexerRecords(dslContext, objectId, content, version);
    return queryExecutor.getDelegate(sqlClient -> {
      List<Tuple> batch = new ArrayList<>();
      marcIndexerRecords.stream()
        .forEach(r -> batch.add(Tuple.of(r.value1(),
          r.value2(),
          r.value3(),
          r.value4(),
          r.value5(),
          r.value6(),
          r.value7())));

      String sql = queryExecutor.toPreparedQuery(
        dslContext.
          insertInto(MARC_INDEXERS, MARC_INDEXERS.fields())
          .values(MARC_INDEXERS.newRecord().intoArray()));

      // Execute the prepared batch
      return sqlClient
        .preparedQuery(sql)
        .executeBatch(batch);
    }).map(true);
  }

  /**
   * Convert a parsed record into rows for MARC_INDEXERS table
   *
   * difference between this java version and the sql version are as follows:
   * - all valued are trimmed
   */
  protected static Collection<MarcIndexersRecord>
  createMarcIndexerRecords(DSLContext dsl, UUID marcId, JsonObject content, int version) {
    JsonArray fieldsArray = content.getJsonArray("fields");
    if (fieldsArray == null) {
      throw new IllegalArgumentException("Content does not contain 'fields' property");
    }
    Set<MarcIndexersRecord> indexerRecords = new HashSet<>();

    for (int i = 0; i < fieldsArray.size(); i++) {
      JsonObject field = fieldsArray.getJsonObject(i);

      for (String fieldNo : field.fieldNames()) {
        Object fieldValueObj = field.getValue(fieldNo);
        JsonObject fieldValue = fieldValueObj instanceof JsonObject ? (JsonObject) fieldValueObj: null;
        String ind1 = fieldValue != null && fieldValue.containsKey("ind1") && !StringUtils.isBlank(fieldValue.getString("ind1"))
          ? fieldValue.getString("ind1").trim() : "#";
        String ind2 = fieldValue != null && fieldValue.containsKey("ind2") && !StringUtils.isBlank(fieldValue.getString("ind2"))
          ? fieldValue.getString("ind2").trim() : "#";

        if (fieldValue != null && fieldValue.containsKey("subfields")) {
          JsonArray subfieldsArray = fieldValue.getJsonArray("subfields");
          for (int j = 0; j < subfieldsArray.size(); j++) {
            JsonObject subfield = subfieldsArray.getJsonObject(j);
            for (String subfieldNo : subfield.fieldNames()) {
              String subfieldValue = subfield.getString(subfieldNo);
              subfieldValue = trimQuotes(subfieldValue.trim());
              MarcIndexersRecord record = createMarcIndexersRecord(dsl, marcId, version, fieldNo, ind1, ind2, subfieldNo, subfieldValue);
              indexerRecords.add(record);
            }
          }
        } else {
          String value = trimQuotes(fieldValueObj.toString().trim());
          MarcIndexersRecord record = createMarcIndexersRecord(dsl, marcId, version, fieldNo, ind1, ind2, "0", value);
          indexerRecords.add(record);
        }
      }
    }

    return indexerRecords;
  }

  private static MarcIndexersRecord createMarcIndexersRecord(DSLContext dsl, UUID marcId, int version, String fieldNo, String ind1, String ind2, String subfieldNo, String value) {
    MarcIndexersRecord record = dsl.newRecord(MARC_INDEXERS);
    record.setFieldNo(fieldNo);
    record.setInd1(ind1);
    record.setInd2(ind2);
    record.setSubfieldNo(subfieldNo);
    record.setValue(value);
    record.setMarcId(marcId);
    record.setVersion(version);
    return record;
  }

  private static String trimQuotes(String value) {
    if (value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
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
      if (Objects.nonNull(leader) && leader.length() > LEADER_STATUS_SUBFIELD_POSITION) {
        return String.valueOf(leader.charAt(LEADER_STATUS_SUBFIELD_POSITION));
      }
    }
    return null;
  }

  /**
   * Update MARC Leader status 05 for the given {@link ParsedRecord} content
   *
   * @param parsedRecord parsedRecord parsed record
   * @param status new MARC Leader status
   */
  public static void updateLeaderStatus(ParsedRecord parsedRecord, Character status) {
    if (Objects.isNull(parsedRecord) || Objects.isNull(parsedRecord.getContent()) || Objects.isNull(status)) {
      return;
    }

    JsonObject marcJson = normalize(parsedRecord.getContent());
    String leader = marcJson.getString(LEADER);
    if (Objects.nonNull(leader) && leader.length() > LEADER_STATUS_SUBFIELD_POSITION) {
      StringBuilder builder = new StringBuilder(leader);
      builder.setCharAt(LEADER_STATUS_SUBFIELD_POSITION, status);
      marcJson.put(LEADER, builder.toString());
      parsedRecord.setContent(normalize(marcJson));
    }
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
   * @param r record
   * @return record type defaulting to MARC
   */
  public static RecordType toRecordType(Record r) {
    if (Objects.nonNull(r.getRecordType())) {
      return RecordType.valueOf(r.getRecordType().toString());
    }
    return RecordType.MARC_BIB;
  }

  public static JsonObject normalize(Object content) {
    return (content instanceof String)
      ? new JsonObject((String) content)
      : JsonObject.mapFrom(content);
  }

}
