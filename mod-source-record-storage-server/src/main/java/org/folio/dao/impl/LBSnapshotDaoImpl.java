package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.SNAPSHOTS_TABLE_NAME;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

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
public class LBSnapshotDaoImpl implements LBSnapshotDao {

  private static final Logger LOG = LoggerFactory.getLogger(LBSnapshotDaoImpl.class);

  public static final String STATUS_COLUMN_NAME = "status";
  public static final String PROCESSING_STARTED_DATE_COLUMN_NAME = "processing_started_date";

  private final PostgresClientFactory pgClientFactory;

  public LBSnapshotDaoImpl(@Autowired PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  public PostgresClient getPostgresClient(String tenantId) {
    return pgClientFactory.createInstance(tenantId);
  }

  @Override
  public String getTableName() {
    return SNAPSHOTS_TABLE_NAME;
  }

  @Override
  public String getId(Snapshot snapshot) {
    return snapshot.getJobExecutionId();
  }

  @Override
  public String getColumns() {
    return ColumnBuilder.of(ID_COLUMN_NAME)
      .append(STATUS_COLUMN_NAME)
      .append(PROCESSING_STARTED_DATE_COLUMN_NAME)
      .build();
  }

  @Override
  public JsonArray toParams(Snapshot snapshot, boolean generateIdIfNotExists) {
    if (generateIdIfNotExists && StringUtils.isEmpty(snapshot.getJobExecutionId())) {
      snapshot.setJobExecutionId(UUID.randomUUID().toString());
    }
    JsonArray params = new JsonArray()
      .add(snapshot.getJobExecutionId())
      .add(snapshot.getStatus());
    if (snapshot.getProcessingStartedDate() != null) {
      params.add(DATE_FORMATTER.format(snapshot.getProcessingStartedDate()));
    } else {
      params.addNull();
    }
    return params;
  }

  @Override
  public SnapshotCollection toCollection(ResultSet resultSet) {
    return new SnapshotCollection()
      .withSnapshots(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  public Snapshot toBean(JsonObject result) {
    Snapshot snapshot = new Snapshot().withJobExecutionId(result.getString(ID_COLUMN_NAME))
      .withStatus(Snapshot.Status.fromValue(result.getString(STATUS_COLUMN_NAME)));
    Instant processingStartedDate = result.getInstant(PROCESSING_STARTED_DATE_COLUMN_NAME);
    if (processingStartedDate != null) {
      snapshot.setProcessingStartedDate(Date.from(processingStartedDate));
    }
    return snapshot;
  }

}