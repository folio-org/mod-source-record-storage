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
public class LBRecordDaoUtil {

  private final static String COMMA = ",";

  private LBRecordDaoUtil() { }

  /**
   * Get {@link Condition} for provided external id and {@link ExternalIdType}
   * 
   * @param externalId     external id
   * @param externalIdType external id type
   * @return condition
   */
  public static Condition getExternalIdCondition(String externalId, ExternalIdType externalIdType) {
    // NOTE: would be nice to be able to do this without a switch statement
    Condition condition;
    switch (externalIdType) {
      case INSTANCE:
        condition = RECORDS_LB.INSTANCE_ID.eq(UUID.fromString(externalId));
        break;
      case RECORD:
        condition = RECORDS_LB.ID.eq(UUID.fromString(externalId));
        break;
      default:
        throw new BadRequestException(String.format("Unknown external id type %s", externalIdType));
    }
    return condition;
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
          .map(r -> LBRecordDaoUtil.toRecord(r.unwrap())));
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
      .where(condition))
        .map(LBRecordDaoUtil::toOptionalRecord);
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
        .map(LBRecordDaoUtil::toOptionalRecord);
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
        .map(LBRecordDaoUtil::toSingleRecord);
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
        .map(LBRecordDaoUtil::toSingleOptionalRecord)
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
      .withSnapshotId(record.getSnapshotId())
      .withRecordType(org.folio.rest.jaxrs.model.SourceRecord.RecordType.valueOf(record.getRecordType().toString()))
      .withOrder(record.getOrder());
    return sourceRecord
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
    Record record = new Record()
      .withId(pojo.getId().toString())
      .withSnapshotId(pojo.getSnapshotId().toString())
      .withMatchedId(pojo.getMatchedId().toString())
      .withRecordType(org.folio.rest.jaxrs.model.Record.RecordType.valueOf(pojo.getRecordType().toString()))
      .withState(org.folio.rest.jaxrs.model.Record.State.valueOf(pojo.getState().toString()))
      .withOrder(pojo.getOrderInFile())
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
    dbRecord.setOrderInFile(record.getOrder());
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
   * Get {@link Condition} to filter by snapshot id
   * 
   * @param snapshotId snapshot id to equal
   * @return condition
   */
  public static Condition conditionFilterBy(String snapshotId) {
    final Condition condition = DSL.trueCondition();
    if (StringUtils.isNoneEmpty(snapshotId)) {
      condition.and(RECORDS_LB.SNAPSHOT_ID.eq(UUID.fromString(snapshotId)));
    }
    return condition;
  }

  /**
   * Get {@link Condition} to filter by combination of properties using only 'and'
   * 
   * @param instanceId            instance id to equal
   * @param recordType            record type to equal
   * @param suppressFromDiscovery suppress from discovery to equal
   * @param updatedAfter          updated after to be greater than or equal
   * @param updatedBefore         updated before to be less than or equal
   * @return condition
   */
  public static Condition conditionFilterBy(String instanceId, String recordType, boolean suppressFromDiscovery,
    Date updatedAfter, Date updatedBefore) {
    Condition condition = DSL.trueCondition();
    if (StringUtils.isNoneEmpty(instanceId)) {
      condition.and(RECORDS_LB.INSTANCE_ID.eq(UUID.fromString(instanceId)));
    }
    if (StringUtils.isNoneEmpty(recordType)) {
      condition.and(RECORDS_LB.RECORD_TYPE.eq(RecordType.valueOf(recordType)));
    }
    if (StringUtils.isNoneEmpty(instanceId)) {
      condition.and(RECORDS_LB.SUPPRESS_DISCOVERY.eq(suppressFromDiscovery));
    }
    if (Objects.nonNull(updatedAfter)) {
      condition.and(RECORDS_LB.UPDATED_DATE.greaterOrEqual(updatedAfter.toInstant().atOffset(ZoneOffset.UTC)));
    }
    if (Objects.nonNull(updatedBefore)) {
      condition.and(RECORDS_LB.UPDATED_DATE.lessOrEqual(updatedBefore.toInstant().atOffset(ZoneOffset.UTC)));
    }
    return condition;
  }

  /**
   * Convert {@link List} of {@link String} to {@link List} or {@link OrderField}
   * 
   * @param orderBy list of order strings i.e. 'order,ASC' or 'state'
   * @return list of order fields
   */
  public static List<OrderField<?>> toOrderFields(List<String> orderBy) {
    return orderBy.stream()
      .map(order -> order.split(COMMA))
      .map(order -> RECORDS_LB.field(toColumnName(order[0])).sort(order.length > 1
        ? SortOrder.valueOf(order[1]) : SortOrder.DEFAULT))
      .collect(Collectors.toList());
  }

  private static Record toSingleRecord(RowSet<Row> rows) {
    return toRecord(rows.iterator().next());
  }

  private static Optional<Record> toSingleOptionalRecord(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toRecord(rows.iterator().next())) : Optional.empty();
  }

  /**
   * Relies on strong convention between dto property name and database column name.
   * Property name being lower camel case and column name being lower snake case of the property name.
   */
  private static String toColumnName(String propertyName) {
    return LOWER_CAMEL.to(LOWER_UNDERSCORE, propertyName);
  }

}