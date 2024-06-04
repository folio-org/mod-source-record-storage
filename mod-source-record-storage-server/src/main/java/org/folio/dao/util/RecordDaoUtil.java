package org.folio.dao.util;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static java.lang.String.format;

import static org.folio.rest.jooq.Tables.RECORDS_LB;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.StrippedParsedRecord;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

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

/**
 * Utility class for managing {@link Record}
 */
public final class RecordDaoUtil {

  public static final String RECORD_NOT_FOUND_TEMPLATE = "Record with id '%s' was not found";

  private static final String COMMA = ",";
  private static final List<String> DELETED_LEADER_RECORD_STATUS = Arrays.asList("d", "s", "x");

  private RecordDaoUtil() {}

  /**
   * Get {@link Condition} for provided external id and {@link IdType}
   *
   * @param externalId external id
   * @param idType     external id type
   * @return condition
   */
  public static Condition getExternalIdCondition(String externalId, IdType idType) {
    return getIdCondition(idType, idField -> idField.eq(toUUID(externalId)));
  }

  /**
   * Get {@link Condition} where in external list ids and {@link IdType}
   *
   * @param externalIds list of external id
   * @param idType      external id type
   * @return condition
   */
  public static Condition getExternalIdsCondition(List<String> externalIds, IdType idType) {
    return getIdCondition(idType, idField -> idField.in(toUUIDs(externalIds)));
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
  public static Future<Optional<Record>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor,
                                                         Condition condition) {
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
        throw new NotFoundException(format(RECORD_NOT_FOUND_TEMPLATE, record.getId()));
      });
  }

  /**
   * Make sure record has id.
   *
   * @param record record
   * @return record with id
   */
  public static Record ensureRecordHasId(Record record) {
    if (Objects.isNull(record.getId())) {
      record.setId(UUID.randomUUID().toString());
    }
    return record;
  }

  /**
   * Make sure record has matched id.
   *
   * @param record record
   * @return record with matched id
   */
  public static Record ensureRecordHasMatchedId(Record record) {
    if (Objects.isNull(record.getMatchedId())) {
      record.setMatchedId(UUID.randomUUID().toString());
    }
    return record;
  }

  /**
   * Make sure record has additional info suppress discovery.
   *
   * @param record record
   * @return record with additional info suppress discovery
   */
  public static Record ensureRecordHasSuppressDiscovery(Record record) {
    if (Objects.isNull(record.getAdditionalInfo()) || Objects.isNull(record.getAdditionalInfo().getSuppressDiscovery())) {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false));
    }
    return record;
  }

  /**
   * Make sure all associated records have record id for foreign key.
   *
   * @param record record
   * @return record with all foreign keys set
   */
  public static Record ensureRecordForeignKeys(Record record) {
    if (Objects.nonNull(record.getRawRecord())) {
      record.getRawRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getParsedRecord())) {
      record.getParsedRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getErrorRecord())) {
      record.getErrorRecord().setId(record.getId());
    }
    return record;
  }

  /**
   * Convert database query result {@link Row} to {@link SourceRecord}
   *
   * @param row query result row
   * @return SourceRecord
   */
  public static SourceRecord toSourceRecord(Row row) {
    RecordsLb pojo = RowMappers.getRecordsLbMapper().apply(row);
    SourceRecord sourceRecord = new SourceRecord();
    if (Objects.nonNull(pojo.getId())) {
      sourceRecord.withRecordId(pojo.getId().toString());
    }
    if (Objects.nonNull(pojo.getSnapshotId())) {
      sourceRecord.withSnapshotId(pojo.getSnapshotId().toString());
    }
    if (Objects.nonNull(pojo.getRecordType())) {
      sourceRecord.withRecordType(SourceRecord.RecordType.valueOf(pojo.getRecordType().toString()));
    }

    sourceRecord.withOrder(pojo.getOrder())
      .withDeleted((Objects.nonNull(pojo.getState()) && State.valueOf(pojo.getState().toString()).equals(State.DELETED))
        || DELETED_LEADER_RECORD_STATUS.contains(pojo.getLeaderRecordStatus()));

    return sourceRecord
      .withAdditionalInfo(toAdditionalInfo(pojo))
      .withExternalIdsHolder(toExternalIdsHolder(pojo))
      .withMetadata(toMetadata(pojo));
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
      sourceRecord.withRecordType(SourceRecord.RecordType.valueOf(record.getRecordType().toString()));
    }
    if (Objects.nonNull(record.getState())) {
      sourceRecord.withDeleted(record.getState().equals(State.DELETED));
    }
    return sourceRecord
      .withOrder(record.getOrder())
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
      record.withRecordType(Record.RecordType.valueOf(pojo.getRecordType().toString()));
    }
    if (Objects.nonNull(pojo.getState())) {
      record.withState(State.valueOf(pojo.getState().toString()));
    }

    record
      .withOrder(pojo.getOrder())
      .withGeneration(pojo.getGeneration())
      .withLeaderRecordStatus(pojo.getLeaderRecordStatus())
      .withDeleted(record.getState().equals(State.DELETED)
        || DELETED_LEADER_RECORD_STATUS.contains(record.getLeaderRecordStatus()));

    return record
      .withAdditionalInfo(toAdditionalInfo(pojo))
      .withExternalIdsHolder(toExternalIdsHolder(pojo))
      .withMetadata(toMetadata(pojo));
  }

  //TODO: Update JOOQ versions, or replace RowMappers
  // Since Vert.x v3.9.4 Row methods throws NoSuchElementException instead of returning null
  // So RowMappers can't be used when not all columns where selected
  public static StrippedParsedRecord toStrippedParsedRecord(Row row) {
    RecordsLb pojo = new RecordsLb()
      .setId(row.getUUID("id"))
      .setRecordType(RecordType.valueOf(row.getString("record_type")))
      .setExternalId(row.getUUID("external_id"))
      .setState(RecordState.valueOf(row.getString("state")));

    return new StrippedParsedRecord()
      .withId(pojo.getId().toString())
      .withRecordType(StrippedParsedRecord.RecordType.valueOf(pojo.getRecordType().toString()))
      .withRecordState(StrippedParsedRecord.RecordState.valueOf(pojo.getState().toString()))
      .withExternalIdsHolder(toExternalIdsHolder(pojo));
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

  public static String getExternalId(ExternalIdsHolder externalIdsHolder, Record.RecordType recordType) {
    if (Objects.isNull(externalIdsHolder)) {
      return null;
    }
    if (Record.RecordType.MARC_BIB == recordType) {
      return externalIdsHolder.getInstanceId();
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return externalIdsHolder.getHoldingsId();
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      return externalIdsHolder.getAuthorityId();
    }
    return null;
  }

  public static IdType getExternalIdType(Record.RecordType recordType) {
    if (Record.RecordType.MARC_BIB == recordType) {
      return IdType.INSTANCE;
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return IdType.HOLDINGS;
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      return IdType.AUTHORITY;
    }
    return null;
  }

  public static String getExternalHrid(ExternalIdsHolder externalIdsHolder, Record.RecordType recordType) {
    if (Objects.isNull(externalIdsHolder)) {
      return null;
    }
    if (Record.RecordType.MARC_BIB == recordType) {
      return externalIdsHolder.getInstanceHrid();
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return externalIdsHolder.getHoldingsHrid();
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      return externalIdsHolder.getAuthorityHrid();
    }
    return null;
  }

  /**
   * Convert {@link Record} to database record {@link RecordsLbRecord}
   *
   * @param record record
   * @return RecordsLbRecord
   */
  public static RecordsLbRecord toDatabaseRecord(Record record) {
    RecordsLbRecord dbRecord = new RecordsLbRecord();
    var recordType = record.getRecordType();
    if (StringUtils.isNotEmpty(record.getId())) {
      dbRecord.setId(UUID.fromString(record.getId()));
    }
    if (StringUtils.isNotEmpty(record.getSnapshotId())) {
      dbRecord.setSnapshotId(UUID.fromString(record.getSnapshotId()));
    }
    if (StringUtils.isNotEmpty(record.getMatchedId())) {
      dbRecord.setMatchedId(UUID.fromString(record.getMatchedId()));
    }
    if (Objects.nonNull(recordType)) {
      dbRecord.setRecordType(RecordType.valueOf(recordType.toString()));
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
    var externalId = getExternalId(record.getExternalIdsHolder(), recordType);
    if (StringUtils.isNotEmpty(externalId)) {
      dbRecord.setExternalId(UUID.fromString(externalId));
    }
    var externalHrid = getExternalHrid(record.getExternalIdsHolder(), recordType);
    if (StringUtils.isNotEmpty(externalHrid)) {
      dbRecord.setExternalHrid(externalHrid);
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
   * @param externalId instance id to equal
   * @return condition
   */
  public static Condition filterRecordByExternalId(String externalId) {
    if (StringUtils.isNotEmpty(externalId)) {
      return RECORDS_LB.EXTERNAL_ID.eq(toUUID(externalId));
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by external entity hrid
   *
   * @param externalHrid external entity hrid to equal
   * @return condition
   */
  public static Condition filterRecordByExternalHrid(String externalHrid) {
    if (StringUtils.isNotEmpty(externalHrid)) {
      return RECORDS_LB.EXTERNAL_HRID.eq(externalHrid);
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by external entity hrid using specified values
   *
   * @param externalHridValues external entity hrid values to equal
   * @return condition
   */
  public static Condition filterRecordByExternalHridValues(List<String> externalHridValues) {
    return RECORDS_LB.EXTERNAL_HRID.in(externalHridValues);
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
      return filterRecordByType(toRecordType(type));
    }
    return DSL.noCondition();
  }

  /**
   * Get {@link Condition} to filter by type
   *
   * @param recordType type to equal
   * @return condition
   */
  public static Condition filterRecordByType(RecordType recordType) {
    return RECORDS_LB.RECORD_TYPE.eq(recordType);
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
   * Get {@link Condition} to filter by leader record status list
   *
   * @param statuses leader record statuses to equal
   * @return condition
   */
  public static Condition filterRecordByLeaderRecordStatus(List<String> statuses) {
    if (ObjectUtils.isNotEmpty(statuses)) {
      return RECORDS_LB.LEADER_RECORD_STATUS.in(statuses);
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
   * Get {@link Condition} to filter by state ACTUAL or DELETED or ACTUAL and leader record status d, s, or x
   *
   * @param deleted deleted flag
   * @return condition
   */
  public static Condition filterRecordByDeleted(Boolean deleted) {
    if (deleted == null) {
      return null;
    }
    Condition condition = filterRecordByState(RecordState.ACTUAL.name());
    if (Boolean.TRUE.equals(deleted)) {
      condition = condition.or(filterRecordByState(RecordState.DELETED.name()))
        .or(filterRecordByState(RecordState.ACTUAL.name()).and(filterRecordByLeaderRecordStatus(DELETED_LEADER_RECORD_STATUS)));
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

  public static Condition filterRecordByExternalIdNonNull() {
    return RECORDS_LB.EXTERNAL_ID.isNotNull();
  }

  /**
   * Convert {@link List} of {@link String} to {@link List} or {@link OrderField}
   * <p>
   * Relies on strong convention between dto property name and database column name.
   * Property name being lower camel case and column name being lower snake case of the property name.
   *
   * @param orderBy   list of order strings i.e. 'order,ASC' or 'state'
   * @param forOffset flag to ensure an order is applied
   * @return list of sort fields
   */
  @SuppressWarnings("squid:S1452")
  public static List<OrderField<?>> toRecordOrderFields(List<String> orderBy, Boolean forOffset) {
    if (forOffset && orderBy.isEmpty()) {
      return Arrays.asList(new OrderField<?>[]{RECORDS_LB.ID.asc()});
    }
    return orderBy.stream()
      .map(order -> order.split(COMMA))
      .map(order -> {
        try {
          return RECORDS_LB.field(LOWER_CAMEL.to(LOWER_UNDERSCORE, order[0]))
            .sort(order.length > 1 ? SortOrder.valueOf(order[1]) : SortOrder.DEFAULT);
        } catch (Exception e) {
          throw new BadRequestException(format("Invalid order by %s", String.join(",", order)));
        }
      })
      .collect(Collectors.toList());
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
      throw new BadRequestException(format("Invalid UUID %s", uuid));
    }
  }

  private static List<UUID> toUUIDs(List<String> uuids) {
    return uuids.stream().map(RecordDaoUtil::toUUID).collect(Collectors.toList());
  }

  private static RecordType toRecordType(String type) {
    try {
      return RecordType.valueOf(type);
    } catch (Exception e) {
      throw new BadRequestException(format("Unknown record type %s", type));
    }
  }

  private static RecordState toRecordState(String state) {
    try {
      return RecordState.valueOf(state);
    } catch (Exception e) {
      throw new BadRequestException(format("Unknown record state %s", state));
    }
  }

  private static AdditionalInfo toAdditionalInfo(RecordsLb pojo) {
    AdditionalInfo additionalInfo = new AdditionalInfo();
    if (Objects.nonNull(pojo.getSuppressDiscovery())) {
      additionalInfo.withSuppressDiscovery(pojo.getSuppressDiscovery());
    }
    return additionalInfo;
  }

  private static ExternalIdsHolder toExternalIdsHolder(RecordsLb pojo) {
    ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder();
    var externalIdOptional = Optional.ofNullable(pojo.getExternalId()).map(UUID::toString);
    var externalHridOptional = Optional.ofNullable(pojo.getExternalHrid());
    if (RecordType.MARC_BIB == pojo.getRecordType()) {
      externalIdOptional.ifPresent(externalIdsHolder::setInstanceId);
      externalHridOptional.ifPresent(externalIdsHolder::setInstanceHrid);
    } else if (RecordType.MARC_HOLDING == pojo.getRecordType()) {
      externalIdOptional.ifPresent(externalIdsHolder::setHoldingsId);
      externalHridOptional.ifPresent(externalIdsHolder::setHoldingsHrid);
    } else if (RecordType.MARC_AUTHORITY == pojo.getRecordType()) {
      externalIdOptional.ifPresent(externalIdsHolder::setAuthorityId);
      externalHridOptional.ifPresent(externalIdsHolder::setAuthorityHrid);
    }
    return externalIdsHolder;
  }

  private static Metadata toMetadata(RecordsLb pojo) {
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
    return metadata;
  }

  private static Condition getRecordTypeCondition(RecordType recordType) {
    if (recordType == null) {
      return DSL.noCondition();
    }
    return RECORDS_LB.RECORD_TYPE.eq(recordType);
  }

  private static Condition getIdCondition(IdType idType, Function<Field<UUID>, Condition> idFieldToConditionMapper) {
    IdType idTypeToUse = idType;
    RecordType recordType = null;
    if (idType == IdType.HOLDINGS) {
      idTypeToUse = IdType.EXTERNAL;
      recordType = RecordType.MARC_HOLDING;
    } else if (idType == IdType.INSTANCE) {
      idTypeToUse = IdType.EXTERNAL;
      recordType = RecordType.MARC_BIB;
    } else if (idType == IdType.AUTHORITY) {
      idTypeToUse = IdType.EXTERNAL;
      recordType = RecordType.MARC_AUTHORITY;
    }
    var idField = RECORDS_LB.field(LOWER_CAMEL.to(LOWER_UNDERSCORE, idTypeToUse.getIdField()), UUID.class);
    return idFieldToConditionMapper.apply(idField).and(getRecordTypeCondition(recordType));
  }

}
