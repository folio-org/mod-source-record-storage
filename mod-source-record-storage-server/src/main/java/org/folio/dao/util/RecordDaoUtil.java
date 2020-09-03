package org.folio.dao.util;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static org.folio.rest.jooq.Tables.RECORDS_LB;

import java.time.ZoneOffset;
import java.util.Arrays;
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
public final class RecordDaoUtil {

  private static final String COMMA = ",";

  private static final List<String> DELETED_LEADER_RECORD_STATUS = Arrays.asList("d", "s", "x");

  private RecordDaoUtil() { }

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
   * Get {@link Condition} where in external list ids and {@link ExternalIdType}
   *
   * @param externalIds    list of external id
   * @param externalIdType external id type
   * @return condition
   */
  public static Condition getExternalIdCondition(List<String> externalIds, ExternalIdType externalIdType) {
    return RECORDS_LB.field(LOWER_CAMEL.to(LOWER_UNDERSCORE, externalIdType.getExternalIdField()), UUID.class).in(toUUIDs(externalIds));
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
          .map(r -> RecordDaoUtil.toRecord(r.unwrap())));
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
        .map(RecordDaoUtil::toOptionalRecord);
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
      .where(RECORDS_LB.ID.eq(toUUID(id))))
        .map(RecordDaoUtil::toOptionalRecord);
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
        .map(RecordDaoUtil::toSingleRecord);
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
      .where(RECORDS_LB.ID.eq(toUUID(record.getId())))
      .returning())
        .map(RecordDaoUtil::toSingleOptionalRecord)
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
      .withRecordId(record.getMatchedId())
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
    }
    record
      .withOrder(pojo.getOrder())
      .withGeneration(pojo.getGeneration())
      .withLeaderRecordStatus(pojo.getLeaderRecordStatus());
    record.withDeleted(record.getState().equals(State.DELETED)
      || DELETED_LEADER_RECORD_STATUS.contains(record.getLeaderRecordStatus()));
    AdditionalInfo additionalInfo = new AdditionalInfo();
    if (Objects.nonNull(pojo.getSuppressDiscovery())) {
      additionalInfo.withSuppressDiscovery(pojo.getSuppressDiscovery());
    }
    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder();
    if (Objects.nonNull(pojo.getInstanceId())) {
      externalIdsHolder.withInstanceId(pojo.getInstanceId().toString());
    }
    if (Objects.nonNull(pojo.getInstanceHrid())) {
      externalIdsHolder.withInstanceHrid(pojo.getInstanceHrid().toString());
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
    dbRecord.setLeaderRecordStatus(record.getLeaderRecordStatus());
    if (Objects.nonNull(record.getAdditionalInfo())) {
      dbRecord.setSuppressDiscovery(record.getAdditionalInfo().getSuppressDiscovery());
    }
    if (Objects.nonNull(record.getExternalIdsHolder()) && StringUtils.isNotEmpty(record.getExternalIdsHolder().getInstanceId())) {
      dbRecord.setInstanceId(UUID.fromString(record.getExternalIdsHolder().getInstanceId()));
    }
    if (Objects.nonNull(record.getExternalIdsHolder()) && StringUtils.isNotEmpty(record.getExternalIdsHolder().getInstanceHrid())) {
      dbRecord.setInstanceHrid(UUID.fromString(record.getExternalIdsHolder().getInstanceHrid()));
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
   * Get {@link Condition} to filter by record id
   *
   * @param recordId record id to equal
   * @return condition
   */
  public static Condition filterRecordByRecordId(String recordId) {
    if (StringUtils.isNotEmpty(recordId)) {
      return RECORDS_LB.MATCHED_ID.eq(toUUID(recordId));
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by instance id
   *
   * @param instanceId instance id to equal
   * @return condition
   */
  public static Condition filterRecordByInstanceId(String instanceId) {
    if (StringUtils.isNotEmpty(instanceId)) {
      return RECORDS_LB.INSTANCE_ID.eq(toUUID(instanceId));
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by instance hrid
   *
   * @param instanceHrid instance id to equal
   * @return condition
   */
  public static Condition filterRecordByInstanceHrid(String instanceHrid) {
    if (StringUtils.isNotEmpty(instanceHrid)) {
      return RECORDS_LB.INSTANCE_HRID.eq(toUUID(instanceHrid));
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by snapshotId id
   *
   * @param snapshotId snapshot id to equal
   * @return condition
   */
  public static Condition filterRecordBySnapshotId(String snapshotId) {
    if (StringUtils.isNotEmpty(snapshotId)) {
      return RECORDS_LB.SNAPSHOT_ID.eq(toUUID(snapshotId));
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by type
   *
   * @param type type to equal
   * @return condition
   */
  public static Condition filterRecordByType(String type) {
    if (StringUtils.isNotEmpty(type)) {
      return RECORDS_LB.RECORD_TYPE.eq(toRecordType(type));
    }
    return DSL.noCondition();
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
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by suppressFromDiscovery
   *
   * @param suppressFromDiscovery suppressFromDiscovery to equal
   * @return condition
   */
  public static Condition filterRecordBySuppressFromDiscovery(Boolean suppressFromDiscovery) {
    if (Objects.nonNull(suppressFromDiscovery)) {
      return RECORDS_LB.SUPPRESS_DISCOVERY.eq(suppressFromDiscovery);
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by leader record status
   *
   * @param leaderRecordStatus leader record status to equal
   * @return condition
   */
  public static Condition filterRecordByLeaderRecordStatus(String leaderRecordStatus) {
    if (StringUtils.isNotEmpty(leaderRecordStatus)) {
      return RECORDS_LB.LEADER_RECORD_STATUS.eq(leaderRecordStatus);
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by range of updated date
   *
   * @param updatedAfter  updated after to be greater than or equal
   * @param updatedBefore updated before to be less than or equal
   * @return condition
   */
  public static Condition filterRecordByUpdatedDateRange(Date updatedAfter, Date updatedBefore) {
    Condition condition = DSL.noCondition();
    if (Objects.nonNull(updatedAfter)) {
      condition = RECORDS_LB.UPDATED_DATE.greaterOrEqual(updatedAfter.toInstant().atOffset(ZoneOffset.UTC));
    }
    if (Objects.nonNull(updatedBefore)) {
      condition = condition.and(RECORDS_LB.UPDATED_DATE.lessOrEqual(updatedBefore.toInstant().atOffset(ZoneOffset.UTC)));
    }
    return condition;
  }

  /**
   * Get {@link Condition} to filter by state ACTUAL or DELETED or leader record status d, s, or x
   *
   * @param deleted deleted flag
   * @return condition
   */
  public static Condition filterRecordByDeleted(Boolean deleted) {
    Condition condition = filterRecordByState(RecordState.ACTUAL.name());
    if (Boolean.TRUE.equals(deleted)) {
      condition = condition.or(filterRecordByState(RecordState.DELETED.name()));
      for(String status : DELETED_LEADER_RECORD_STATUS) {
        condition = condition.or(filterRecordByLeaderRecordStatus(status));
      }
    }
    return condition;
  }

  /**
   * Get {@link Condition} to filter by not snapshot id
   *
   * @param snapshotId snapshot id to not equal
   * @return condition
   */
  public static Condition filterRecordByNotSnapshotId(String snapshotId) {
    if (StringUtils.isNotEmpty(snapshotId)) {
      return RECORDS_LB.SNAPSHOT_ID.notEqual(toUUID(snapshotId));
    }
    return DSL.noCondition();
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

  private static List<UUID> toUUIDs(List<String> uuids) {
    return uuids.stream().map(RecordDaoUtil::toUUID).collect(Collectors.toList());
  }

  private static RecordType toRecordType(String type) {
    try {
      return RecordType.valueOf(type);
    } catch (Exception e) {
      throw new BadRequestException(String.format("Unknown record type %s", type));
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
