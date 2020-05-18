package org.folio.dao.impl;

import static java.util.stream.StreamSupport.stream;
import static org.folio.dao.util.DaoUtil.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.SNAPSHOTS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.UPDATED_DATE_COLUMN_NAME;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.AbstractEntityDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.SnapshotQuery;
import org.folio.dao.util.ColumnBuilder;
import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.TupleWrapper;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.springframework.stereotype.Component;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

// <createTable tableName="snapshots_lb">
//   <column name="id" type="uuid">
//     <constraints primaryKey="true" nullable="false"/>
//   </column>
//   <column name="status" type="${database.defaultSchemaName}.job_execution_status">
//     <constraints nullable="false"/>
//   </column>
//   <column name="processing_started_date" type="timestamptz"></column>
// </createTable>
@Component
public class LBSnapshotDaoImpl extends AbstractEntityDao<Snapshot, SnapshotCollection, SnapshotQuery> implements LBSnapshotDao {

  public static final String STATUS_COLUMN_NAME = "status";
  public static final String PROCESSING_STARTED_DATE_COLUMN_NAME = "processing_started_date";

  @Override
  public String getTableName() {
    return SNAPSHOTS_TABLE_NAME;
  }

  @Override
  public String getColumns() {
    return ColumnBuilder
      .of(ID_COLUMN_NAME)
      .append(STATUS_COLUMN_NAME)
      .append(PROCESSING_STARTED_DATE_COLUMN_NAME)
      .append(CREATED_BY_USER_ID_COLUMN_NAME)
      .append(CREATED_DATE_COLUMN_NAME)
      .append(UPDATED_BY_USER_ID_COLUMN_NAME)
      .append(UPDATED_DATE_COLUMN_NAME)
      .build();
  }

  @Override
  public String getId(Snapshot snapshot) {
    return snapshot.getJobExecutionId();
  }

  @Override
  protected Tuple toTuple(Snapshot snapshot, boolean generateIdIfNotExists) {
    if (generateIdIfNotExists && StringUtils.isEmpty(snapshot.getJobExecutionId())) {
      snapshot.setJobExecutionId(UUID.randomUUID().toString());
    }
    TupleWrapper tupleWrapper = TupleWrapper.of()
      .addUUID(snapshot.getJobExecutionId())
      .addEnum(snapshot.getStatus())
      .addOffsetDateTime(snapshot.getProcessingStartedDate());
    if (Objects.nonNull(snapshot.getMetadata())) {
      tupleWrapper.addUUID(snapshot.getMetadata().getCreatedByUserId())
        .addOffsetDateTime(snapshot.getMetadata().getCreatedDate())
        .addUUID(snapshot.getMetadata().getUpdatedByUserId())
        .addOffsetDateTime(snapshot.getMetadata().getUpdatedDate());
    } else {
      tupleWrapper.addNull().addNull().addNull().addNull();
    }
    return tupleWrapper.get();
  }

  @Override
  protected SnapshotCollection toCollection(RowSet<Row> rowSet) {
    return toEmptyCollection(rowSet)
        .withSnapshots(stream(rowSet.spliterator(), false)
          .map(this::toEntity).collect(Collectors.toList()));
  }

  @Override
  protected SnapshotCollection toEmptyCollection(RowSet<Row> rowSet) {
    return new SnapshotCollection()
      .withSnapshots(Collections.emptyList())
      .withTotalRecords(DaoUtil.getTotalRecords(rowSet));
  }

  @Override
  protected Snapshot toEntity(Row row) {
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(row.getUUID(ID_COLUMN_NAME).toString())
      .withStatus(Snapshot.Status.fromValue(row.getString(STATUS_COLUMN_NAME)));
    OffsetDateTime processingStartedDate = row.getOffsetDateTime(PROCESSING_STARTED_DATE_COLUMN_NAME);
    if (Objects.nonNull(processingStartedDate)) {
      snapshot.setProcessingStartedDate(Date.from(processingStartedDate.toInstant()));
    }
    return snapshot
      .withMetadata(DaoUtil.metadataFromRow(row));
  }

}