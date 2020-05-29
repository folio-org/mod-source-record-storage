package org.folio.dao;

import static org.folio.rest.jooq.Tables.RECORDS_LB;

import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
import org.jooq.InsertSetStep;
import org.jooq.InsertValuesStepN;
import org.jooq.OrderField;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public class LBRecordDao {

  public static Future<List<Record>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition,
      Collection<OrderField<?>> orderFields, int offset, int limit) {
    return queryExecutor.executeAny(dsl -> dsl.selectFrom(RECORDS_LB)
      .where(condition)
      .orderBy(orderFields)
      .offset(offset)
      .limit(limit))
        .map(LBRecordDao::toRecords);
  }

  public static Future<Optional<Record>> findByCondition(ReactiveClassicGenericQueryExecutor queryExecutor, Condition condition) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
      .where(condition))
        .map(LBRecordDao::toOptionalRecord);
  }

  public static Future<Optional<Record>> findById(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.findOneRow(dsl -> dsl.selectFrom(RECORDS_LB)
      .where(RECORDS_LB.ID.eq(UUID.fromString(id))))
        .map(LBRecordDao::toOptionalRecord);
  }

  public static Future<Record> save(ReactiveClassicGenericQueryExecutor queryExecutor, Record record) {
    RecordsLbRecord dbRecord = toDatabaseRecord(record);
    return queryExecutor.executeAny(dsl -> dsl.insertInto(RECORDS_LB)
      .set(dbRecord)
      .onDuplicateKeyUpdate()
      .set(dbRecord)
      .returning())
        .map(LBRecordDao::toRecord);
  }

  public static Future<List<Record>> save(ReactiveClassicGenericQueryExecutor queryExecutor, List<Record> snapshots) {
    return queryExecutor.executeAny(dsl -> {
      InsertSetStep<RecordsLbRecord> insertSetStep = dsl.insertInto(RECORDS_LB);
      InsertValuesStepN<RecordsLbRecord> insertValuesStepN = null;
      for (Record Record : snapshots) {
          insertValuesStepN = insertSetStep.values(toDatabaseRecord(Record).intoArray());
      }
      return insertValuesStepN;
    }).map(LBRecordDao::toRecords);
  }

  public static Future<Record> update(ReactiveClassicGenericQueryExecutor queryExecutor, Record record) {
    RecordsLbRecord dbRecord = toDatabaseRecord(record);
    return queryExecutor.executeAny(dsl -> dsl.update(RECORDS_LB)
      .set(dbRecord)
      .where(RECORDS_LB.ID.eq(UUID.fromString(record.getId())))
      .returning())
        .map(LBRecordDao::toOptionalRecord)
        .map(optionalRecord -> {
          if (optionalRecord.isPresent()) {
            return optionalRecord.get();
          }
          throw new RuntimeException(String.format("Record with id '%s' was not found", record.getId()));
        });
  }

  public static Future<Boolean> delete(ReactiveClassicGenericQueryExecutor queryExecutor, String id) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(RECORDS_LB)
      .where(RECORDS_LB.ID.eq(UUID.fromString(id))))
      .map(res -> res == 1);
  }

  public static Future<Integer> deleteAll(ReactiveClassicGenericQueryExecutor queryExecutor) {
    return queryExecutor.execute(dsl -> dsl.deleteFrom(RECORDS_LB));
  }

  public static SourceRecord toSourceRecord(Record record) {
    SourceRecord sourceRecord = new SourceRecord()
      .withRecordId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withRecordType(org.folio.rest.jaxrs.model.SourceRecord.RecordType.valueOf(record.getRecordType().toString()))
      .withOrder(record.getOrder());
      return sourceRecord
        .withAdditionalInfo(record.getAdditionalInfo())
        .withExternalIdsHolder(record.getExternalIdsHolder())
        .withMetadata(record.getMetadata());
  }

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
    if (Objects.nonNull(record.getExternalIdsHolder())) {
      if (StringUtils.isNotEmpty(record.getExternalIdsHolder().getInstanceId())) {
        dbRecord.setInstanceId(UUID.fromString(record.getExternalIdsHolder().getInstanceId()));
      }
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

  private static Record toRecord(RowSet<Row> rows) {
    return toRecord(rows.iterator().next());
  }

  private static List<Record> toRecords(RowSet<Row> rows) {
    return StreamSupport.stream(rows.spliterator(), false).map(LBRecordDao::toRecord).collect(Collectors.toList());
  }

  private static Optional<Record> toOptionalRecord(RowSet<Row> rows) {
    return rows.rowCount() == 1 ? Optional.of(toRecord(rows.iterator().next())) : Optional.empty();
  }

  private static Optional<Record> toOptionalRecord(Row row) {
    return Objects.nonNull(row) ? Optional.of(toRecord(row)) : Optional.empty();
  }

}