package org.folio.dao.util;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static org.folio.rest.jooq.Tables.RECORDS_LB;

import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.rest.jooq.enums.RecordType;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.RecordsLb;
import org.folio.rest.jooq.tables.records.RecordsLbRecord;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

/**
 * Utility class for managing {@link Record}
 */
public class LbRecordDaoUtil {

  private static final String COMMA = ",";

  private LbRecordDaoUtil() { }

  /**
   * Get {@link Condition} for provided external id and {@link ExternalIdType}
   * 
   * @param externalId     external id
   * @param externalIdType external id type
   * @return condition
   */
  public static Condition getExternalIdCondition(String externalId, ExternalIdType externalIdType) {
    return RECORDS_LB.field(LOWER_CAMEL.to(LOWER_UNDERSCORE, externalIdType.getExternalIdField()), UUID.class).eq(toUUID(externalId));
  }

  /**
   * Searches for {@link Record} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   * using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param condition     condition
   * @param orderFields   fields to order by
   * @param offset        offset
   * @param limit         limit
   * @return future with {@link List} of {@link Record}
   */
  public static Future<Stream<Record>> streamByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition,
      Collection<OrderField<?>> orderFields, int offset, int limit) {
    return queryExecutor.query(dsl ->  dsl.selectFrom(RECORDS_LB)
      .where(condition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit))
        .map(res -> res.stream()
          .map(r -> LbRecordDaoUtil.toRecord(r.unwrap())));
  }

  /**
   * Count query by {@link Condition}
   * 
   * @param queryExecutor query executor
   * @param condition     condition
   * @return future with count
   */
  public static Future<Integer> countByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition) {
    return queryExecutor.findOneRow(dsl -> dsl.selectCount()
      .from(RECORDS_LB)
      .where(condition))
        .map(row -> row.getInteger(0));
  }

 /**
   * Searches for {@link Record} by {@link Condition} using {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param queryExecutor query executor
   * @param condition     condition
   * @return future with optional Record
   */
  public static Future<Optional<Record>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
      .where(condition)
      .orderBy(RECORDS_LB.STATE.sort(SortOrder.ASC))
      .limit(1))
        .map(LbRecordDaoUtil::toOptionalRecord);
  }

 /**
   * Searches for {@link Record} by id using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param id            id
   * @return future with optional Record
   */
  public static Future<Optional<Record>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
      .where(RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(LbRecordDaoUtil::toOptionalRecord);
  }

  /**
   * Saves {@link Record} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param record        record
   * @return future with updated Record
   */
  public static Future<Record> save(ReactiveClassicGenericQueryExecutor queryExecutor, Record record) {
    RecordsLbRecord dbRecord = toDatabaseRecord(record);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(LbRecordDaoUtil::toSingleRecord);
  }

  /**
   * Updates {@link Record} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param record        record to update
   * @return future of updated Record
   */
  public static Future<Record> update(ReactiveClassicGenericQueryExecutor queryExecutor, Record record) {
    RecordsLbRecord dbRecord = toDatabaseRecord(record);
    return queryExecutor.executeAny(dsl -> dsl.update(RECORDS_LB)
      .set(dbRecord)
      .where(RECORDS_LB.ID.eq(UUID.fromString(record.getId())))
      .returning())
        .map(LbRecordDaoUtil::toSingleOptionalRecord)
        .map(optionalRecord -> {
          if (optionalRecord.isPresent()) {
            return optionalRecord.get();
          }
          throw new NotFoundException(String.format("Record with id '%s' was not found", record.getId()));
        });
  }

  /**
   * Convert {@link Record} to {@link SourceRecord}
   * 
   * @param record Record
   * @return SourceRecord
   */
  public static SourceRecord toSourceRecord(Record record) {
    SourceRecord sourceRecord = new SourceRecord()
      .withRecordId(record.getId())
      .withSnapshotId(record.getSnapshotId());
    if (Objects.nonNull(record.getRecordType())) {
      sourceRecord.withRecordType(org.folio.rest.jaxrs.model.SourceRecord.RecordType.valueOf(record.getRecordType().toString()));
    }
    if (Objects.nonNull(record.getState())) {
      sourceRecord.withDeleted(record.getState().equals(State.DELETED));
    }
    return sourceRecord
      .withOrder(record.getOrder())
      .withRawRecord(record.getRawRecord())
      .withParsedRecord(record.getParsedRecord())
      .withAdditionalInfo(record.getAdditionalInfo())
      .withExternalIdsHolder(record.getExternalIdsHolder())
      .withMetadata(record.getMetadata());
  }

  /**
   * Convert database query result {@link Row} to {@link Record}
   * 
   * @param row query result row
   * @return Record
   */
  public static Record toRecord(Row row) {
    RecordsLb pojo = RowMappers.getRecordsLbMapper().apply(row);
    Record record = new Record();
    if (Objects.nonNull(pojo.getId())) {
      record.withId(pojo.getId().toString());
    }
    if (Objects.nonNull(pojo.getSnapshotId())) {
      record.withSnapshotId(pojo.getSnapshotId().toString());
    }
    if (Objects.nonNull(pojo.getMatchedId())) {
      record.withMatchedId(pojo.getMatchedId().toString());
    }
    if (Objects.nonNull(pojo.getRecordType())) {
      record.withRecordType(org.folio.rest.jaxrs.model.Record.RecordType.valueOf(pojo.getRecordType().toString()));
    }
    if (Objects.nonNull(pojo.getState())) {
      record.withState(org.folio.rest.jaxrs.model.Record.State.valueOf(pojo.getState().toString()));
      record.withDeleted(record.getState().equals(State.DELETED));
    }
    record
      .withOrder(pojo.getOrder())
      .withGeneration(pojo.getGeneration());
    AdditionalInfo additionalInfo = new AdditionalInfo();
    if (Objects.nonNull(pojo.getSuppressDiscovery())) {
      additionalInfo.withSuppressDiscovery(pojo.getSuppressDiscovery());
    }
    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder();
    if (Objects.nonNull(pojo.getInstanceId())) {
      externalIdsHolder.withInstanceId(pojo.getInstanceId().toString());
    }
    Metadata metadata = new Metadata();
    if (Objects.nonNull(pojo.getCreatedByUserId())) {
      metadata.withCreatedByUserId(pojo.getCreatedByUserId().toString());
    }
    if (Objects.nonNull(pojo.getCreatedDate())) {
      metadata.withCreatedDate(Date.from(pojo.getCreatedDate().toInstant()));
    }
    if (Objects.nonNull(pojo.getUpdatedByUserId())) {
      metadata.withUpdatedByUserId(pojo.getUpdatedByUserId().toString());
    }
    if (Objects.nonNull(pojo.getUpdatedDate())) {
      metadata.withUpdatedDate(Date.from(pojo.getUpdatedDate().toInstant()));
    }
    return record
      .withAdditionalInfo(additionalInfo)
      .withExternalIdsHolder(externalIdsHolder)
      .withMetadata(metadata);
  }

  /**
   * Convert database query result {@link Row} to {@link Optional} {@link Record}
   * 
   * @param row query result row
   * @return optional Record
   */
  public static Optional<Record> toOptionalRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toRecord(row)) : Optional.empty();
  }

  /**
   * Convert {@link Record} to database record {@link RecordsLbRecord}
   * 
   * @param record record
   * @return RecordsLbRecord
   */
  public static RecordsLbRecord toDatabaseRecord(Record record) {
    RecordsLbRecord dbRecord = new RecordsLbRecord();
    if (StringUtils.isNotEmpty(record.getId())) {
      dbRecord.setId(UUID.fromString(record.getId()));
    }
    if (StringUtils.isNotEmpty(record.getSnapshotId())) {
      dbRecord.setSnapshotId(UUID.fromString(record.getSnapshotId()));
    }
    if (StringUtils.isNotEmpty(record.getMatchedId())) {
      dbRecord.setMatchedId(UUID.fromString(record.getMatchedId()));
    }
    if (Objects.nonNull(record.getRecordType())) {
      dbRecord.setRecordType(RecordType.valueOf(record.getRecordType().toString()));
    }
    if (Objects.nonNull(record.getState())) {
      dbRecord.setState(RecordState.valueOf(record.getState().toString()));
    }
    dbRecord.setOrder(record.getOrder());
    dbRecord.setGeneration(record.getGeneration());
    if (Objects.nonNull(record.getAdditionalInfo())) {
      dbRecord.setSuppressDiscovery(record.getAdditionalInfo().getSuppressDiscovery());
    }
    if (Objects.nonNull(record.getExternalIdsHolder()) && StringUtils.isNotEmpty(record.getExternalIdsHolder().getInstanceId())) {
      dbRecord.setInstanceId(UUID.fromString(record.getExternalIdsHolder().getInstanceId()));
    }
    if (Objects.nonNull(record.getMetadata())) {
      if (Objects.nonNull(record.getMetadata().getCreatedByUserId())) {
        dbRecord.setCreatedByUserId(UUID.fromString(record.getMetadata().getCreatedByUserId()));
      }
      if (Objects.nonNull(record.getMetadata().getCreatedDate())) {
        dbRecord.setCreatedDate(record.getMetadata().getCreatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
      if (Objects.nonNull(record.getMetadata().getUpdatedByUserId())) {
        dbRecord.setUpdatedByUserId(UUID.fromString(record.getMetadata().getUpdatedByUserId()));
      }
      if (Objects.nonNull(record.getMetadata().getUpdatedDate())) {
        dbRecord.setUpdatedDate(record.getMetadata().getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
    }
    return dbRecord;
  }

  /**
   * Get {@link Condition} to filter by combination of properties using only 'and'
   * 
   * @param recordId              record id to equal
   * @param recordType            record type to equal
   * @param suppressFromDiscovery suppress from discovery to equal
   * @param deleted               deleted to equal
   * @param updatedAfter          updated after to be greater than or equal
   * @param updatedBefore         updated before to be less than or equal
   * @return condition
   */
  public static Condition filterRecordBy(String recordId, String recordType, Boolean suppressFromDiscovery, Boolean deleted,
      Date updatedAfter, Date updatedBefore) {
    Condition condition = DSL.trueCondition();
    if (StringUtils.isNotEmpty(recordId)) {
      condition = condition.and(RECORDS_LB.ID.eq(toUUID(recordId)));
    }
    if (StringUtils.isNotEmpty(recordType)) {
      condition = condition.and(RECORDS_LB.RECORD_TYPE.eq(toRecordType(recordType)));
    }
    if (Objects.nonNull(suppressFromDiscovery)) {
      condition = condition.and(RECORDS_LB.SUPPRESS_DISCOVERY.eq(suppressFromDiscovery));
    }
    if (Objects.nonNull(deleted)) {
      condition = condition.and(Boolean.TRUE.equals(deleted)
        ? RECORDS_LB.STATE.eq(RecordState.DELETED)
        : RECORDS_LB.STATE.eq(RecordState.ACTUAL));
    }
    if (Objects.nonNull(updatedAfter)) {
      condition = condition.and(RECORDS_LB.UPDATED_DATE.greaterOrEqual(updatedAfter.toInstant().atOffset(ZoneOffset.UTC)));
    }
    if (Objects.nonNull(updatedBefore)) {
      condition = condition.and(RECORDS_LB.UPDATED_DATE.lessOrEqual(updatedBefore.toInstant().atOffset(ZoneOffset.UTC)));
    }
    return condition;
  }

  /**
   * Get {@link Condition} to filter by state
   * 
   * @param state state to equal
   * @return condition
   */
  public static Condition filterRecordByState(String state) {
    if (StringUtils.isNotEmpty(state)) {
      return RECORDS_LB.STATE.eq(toRecordState(state));
    }
    return DSL.trueCondition();
  }

  /**
   * Get {@link Condition} to filter by instance id
   * 
   * @param instanceId instance id to equal
   * @return condition
   */
  public static Condition filterRecordByInstanceId(String instanceId) {
    if (StringUtils.isNotEmpty(instanceId)) {
      return RECORDS_LB.INSTANCE_ID.eq(UUID.fromString(instanceId));
    }
    return DSL.trueCondition();
  }

  /**
   * Get {@link Condition} to filter by snapshotId id
   * 
   * @param snapshotId snapshot id to equal
   * @return condition
   */
  public static Condition filterRecordBySnapshotId(String snapshotId) {
    if (StringUtils.isNotEmpty(snapshotId)) {
      return RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId));
    }
    return DSL.trueCondition();
  }

  /**
   * Get {@link Condition} to filter by not snapshot id
   * 
   * @param notSnapshotId snapshot id to not equal
   * @return condition
   */
  public static Condition filterRecordByNotSnapshotId(String snapshotId) {
    if (StringUtils.isNotEmpty(snapshotId)) {
      return RECORDS_LB.SNAPSHOT_ID.notEqual(UUID.fromString(snapshotId));
    }
    return DSL.trueCondition();
  }

  /**
   * Convert {@link List} of {@link String} to {@link List} or {@link OrderField}
   * 
   * Relies on strong convention between dto property name and database column name.
   * Property name being lower camel case and column name being lower snake case of the property name.
   * 
   * @param orderBy list of order strings i.e. 'order,ASC' or 'state'
   * @return list of sort fields
   */
  @SuppressWarnings("squid:S1452")
  public static List<OrderField<?>> toRecordOrderFields(List<String> orderBy) {
    return orderBy.stream()
      .map(order -> order.split(COMMA))
      .map(order -> {
        try {
          return RECORDS_LB.field(LOWER_CAMEL.to(LOWER_UNDERSCORE, order[0])).sort(order.length > 1
          ? SortOrder.valueOf(order[1]) : SortOrder.DEFAULT);
        } catch (Exception e) {
          throw new BadRequestException(String.format("Invalid order by %s", String.join(",", order)));
        }
      })
      .collect(Collectors.toList());
  }

  /**
   * Tries to convert string to {@link ExternalIdType}, else returns default RECORD
   * 
   * @param externalIdType external id type as string
   * @return external id type
   */
  public static ExternalIdType toExternalIdType(String externalIdType) {
    try {
      return ExternalIdType.valueOf(externalIdType);
    } catch(Exception e) {
      return ExternalIdType.RECORD;
    }
  }

  private static Record toSingleRecord(RowSet<Row> rows) {
    return toRecord(rows.iterator().next());
  }

  private static Optional<Record> toSingleOptionalRecord(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toRecord(rows.iterator().next())) : Optional.empty();
  }

  private static UUID toUUID(String uuid) {
    try {
      return UUID.fromString(uuid);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Invalid UUID %s", uuid));
    }
  }

  private static RecordType toRecordType(String recordType) {
    try {
      return RecordType.valueOf(recordType);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Unknown record type %s", recordType));
    }
  }

  private static RecordState toRecordState(String state) {
    try {
      return RecordState.valueOf(state);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Unknown record state %s", state));
    }
  }

}