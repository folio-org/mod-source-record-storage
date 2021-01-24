package org.folio.dao.util;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static java.lang.String.format;
import static org.folio.rest.jooq.Tables.SNAPSHOTS_LB;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.SnapshotsLb;
import org.folio.rest.jooq.tables.records.SnapshotsLbRecord;
import org.jooq.Condition;
import org.jooq.InsertSetStep;
import org.jooq.InsertValuesStepN;
import org.jooq.OrderField;
import org.jooq.SortOrder;
import org.jooq.impl.DSL;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

/**
 * Utility class for managing {@link Snapshot}
 */
public final class SnapshotDaoUtil {

  private static final String COMMA = ",";

  public static final String SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE = "Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s";
  public static final String SNAPSHOT_NOT_FOUND_TEMPLATE = "Couldn't find snapshot with id %s";

  private SnapshotDaoUtil() { }

  /**
   * Searches for {@link Snapshot} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   * using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param condition     condition
   * @param orderFields   fields to order by
   * @param offset        offset
   * @param limit         limit
   * @return future with {@link List} of {@link Snapshot}
   */
  public static Future<List<Snapshot>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition,
      Collection<OrderField<?>> orderFields, int offset, int limit) {
    return queryExecutor.executeAny(dsl -> dsl.selectFrom(SNAPSHOTS_LB)
      .where(condition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit))
        .map(SnapshotDaoUtil::toSnapshots);
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
      .from(SNAPSHOTS_LB)
      .where(condition))
        .map(row -> row.getInteger(0));
  }

  /**
   * Searches for {@link Snapshot} by id using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param id            id
   * @return future with optional Snapshot
   */
  public static Future<Optional<Snapshot>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(SNAPSHOTS_LB)
      .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(id))))
        .map(SnapshotDaoUtil::toOptionalSnapshot);
  }

  /**
   * Saves {@link Snapshot} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param snapshot      snapshot
   * @return future with updated Snapshot
   */
  public static Future<Snapshot> save(ReactiveClassicGenericQueryExecutor queryExecutor, Snapshot snapshot) {
    SnapshotsLbRecord dbRecord = toDatabaseRecord(setProcessingStartedDate(snapshot));
    return queryExecutor.executeAny(dsl -> dsl.insertInto(SNAPSHOTS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(SnapshotDaoUtil::toSingleSnapshot);
  }

  /**
   * Saves {@link List} of {@link Snapshot} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param snapshots     list of snapshots
   * @return future with updated List of Snapshot
   */
  public static Future<List<Snapshot>> save(ReactiveClassicGenericQueryExecutor queryExecutor, List<Snapshot> snapshots) {
    return queryExecutor.executeAny(dsl -> {
      InsertSetStep<SnapshotsLbRecord> insertSetStep = dsl.insertInto(SNAPSHOTS_LB);
      InsertValuesStepN<SnapshotsLbRecord> insertValuesStepN = null;
      for (Snapshot snapshot : snapshots) {
        SnapshotsLbRecord dbRecord = toDatabaseRecord(setProcessingStartedDate(snapshot));
          insertValuesStepN = insertSetStep.values(dbRecord.intoArray());
      }
      return insertValuesStepN;
    }).map(SnapshotDaoUtil::toSnapshots);
  }

  /**
   * Updates {@link Snapshot} to the db using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param snapshot      snapshot to update
   * @return future of updated Snapshot
   */
  public static Future<Snapshot> update(ReactiveClassicGenericQueryExecutor queryExecutor, Snapshot snapshot) {
    SnapshotsLbRecord dbRecord = toDatabaseRecord(setProcessingStartedDate(snapshot));
    return queryExecutor.executeAny(dsl -> dsl.update(SNAPSHOTS_LB)
      .set(dbRecord)
      .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(snapshot.getJobExecutionId())))
      .returning())
        .map(SnapshotDaoUtil::toSingleOptionalSnapshot)
        .map(optionalSnapshot -> {
          if (optionalSnapshot.isPresent()) {
            return optionalSnapshot.get();
          }
          throw new NotFoundException(format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId()));
        });
  }

  /**
   * Deletes {@link Snapshot} by id using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @param id            id
   * @return future with boolean whether Snapshot deleted
   */
  public static Future<Boolean> delete(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(SNAPSHOTS_LB)
      .where(SNAPSHOTS_LB.ID.eq(UUID.fromString(id))))
      .map(res -> res == 1);
  }

  /**
   * Deletes all {@link Snapshot} using {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param queryExecutor query executor
   * @return future of number of Snapshot deleted
   */
  public static Future<Integer> deleteAll(ReactiveClassicGenericQueryExecutor queryExecutor) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(SNAPSHOTS_LB));
  }

  /**
   * Convert database query result {@link Row} to {@link Snapshot}
   * 
   * @param row query result row
   * @return Snapshot
   */
  public static Snapshot toSnapshot(Row row) {
    SnapshotsLb pojo = RowMappers.getSnapshotsLbMapper().apply(row);
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(pojo.getId().toString())
      .withStatus(Status.fromValue(pojo.getStatus().toString()));
    if (Objects.nonNull(pojo.getProcessingStartedDate())) {
      snapshot.withProcessingStartedDate(Date.from(pojo.getProcessingStartedDate().toInstant()));
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
    return snapshot.withMetadata(metadata);
  }

  /**
   * Convert database query result {@link Row} to {@link Optional} {@link Snapshot}
   * 
   * @param row query result row
   * @return optional Snapshot
   */
  public static Optional<Snapshot> toOptionalSnapshot(Row row) {
    return Objects.nonNull(row) ? Optional.of(toSnapshot(row)) : Optional.empty();
  }

  /**
   * Convert {@link Snapshot} to database record {@link SnapshotsRecord}
   * 
   * @param snapshot snapshot
   * @return SnapshotsRecord
   */
  public static SnapshotsLbRecord toDatabaseRecord(Snapshot snapshot) {
    SnapshotsLbRecord dbRecord = new SnapshotsLbRecord();
    if (StringUtils.isNotEmpty(snapshot.getJobExecutionId())) {
      dbRecord.setId(UUID.fromString(snapshot.getJobExecutionId()));
    }
    if (Objects.nonNull(snapshot.getStatus())) {
      dbRecord.setStatus(JobExecutionStatus.valueOf(snapshot.getStatus().toString()));
    }
    if (Objects.nonNull(snapshot.getProcessingStartedDate())) {
      dbRecord.setProcessingStartedDate(snapshot.getProcessingStartedDate().toInstant().atOffset(ZoneOffset.UTC));
    }
    if (Objects.nonNull(snapshot.getMetadata())) {
      if (Objects.nonNull(snapshot.getMetadata().getCreatedByUserId())) {
        dbRecord.setCreatedByUserId(UUID.fromString(snapshot.getMetadata().getCreatedByUserId()));
      }
      if (Objects.nonNull(snapshot.getMetadata().getCreatedDate())) {
        dbRecord.setCreatedDate(snapshot.getMetadata().getCreatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
      if (Objects.nonNull(snapshot.getMetadata().getUpdatedByUserId())) {
        dbRecord.setUpdatedByUserId(UUID.fromString(snapshot.getMetadata().getUpdatedByUserId()));
      }
      if (Objects.nonNull(snapshot.getMetadata().getUpdatedDate())) {
        dbRecord.setUpdatedDate(snapshot.getMetadata().getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
    }
    return dbRecord;
  }

  /**
   * Get {@link Condition} to filter by snapshot id
   * 
   * @param status snapshot status
   * @return condition
   */
  public static Condition filterSnapshotByStatus(String status) {
    if (StringUtils.isNotEmpty(status)) {
      try {
        return SNAPSHOTS_LB.STATUS.eq(JobExecutionStatus.valueOf(status));
      } catch(Exception e) {
        throw new BadRequestException(format("Unknown job execution status %s", status));
      }
    }
    return DSL.noCondition();
  }

  /**
   * Convert {@link List} of {@link String} to {@link List} or {@link OrderField}
   * 
   * Relies on strong convention between dto property name and database column name.
   * Property name being lower camel case and column name being lower snake case of the property name.
   * 
   * @param orderBy   list of order strings i.e. 'order,ASC' or 'state'
   * @param forOffset flag to ensure an order is applied
   * @return list of order fields
   */
  @SuppressWarnings("squid:S1452")
  public static List<OrderField<?>> toSnapshotOrderFields(List<String> orderBy, Boolean forOffset) {
    if (forOffset && orderBy.isEmpty()) {
      return Arrays.asList(new OrderField<?>[] { SNAPSHOTS_LB.ID.asc() });
    }
    return orderBy.stream()
      .map(order -> order.split(COMMA))
      .map(order -> {
        try {
          return SNAPSHOTS_LB.field(LOWER_CAMEL.to(LOWER_UNDERSCORE, order[0])).sort(order.length > 1
            ? SortOrder.valueOf(order[1]) : SortOrder.DEFAULT);
        } catch (Exception e) {
          throw new BadRequestException(format("Invalid order by %s", String.join(",", order)));
        }
      })
      .collect(Collectors.toList());
  }

  private static Snapshot toSingleSnapshot(RowSet<Row> rows) {
    return toSnapshot(rows.iterator().next());
  }

  private static Optional<Snapshot> toSingleOptionalSnapshot(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toSnapshot(rows.iterator().next())) : Optional.empty();
  }

  private static List<Snapshot> toSnapshots(RowSet<Row> rows) {
    return StreamSupport.stream(rows.spliterator(), false)
      .map(SnapshotDaoUtil::toSnapshot)
      .collect(Collectors.toList());
  }

  private static Snapshot setProcessingStartedDate(Snapshot snapshot) {
    if (Snapshot.Status.PARSING_IN_PROGRESS.equals(snapshot.getStatus())) {
      snapshot.setProcessingStartedDate(new Date());
    }
    return snapshot;
  }

}
