package org.folio.dao.impl;

import static com.fasterxml.jackson.databind.util.StdDateFormat.DATE_FORMAT_STR_ISO8601;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.SNAPSHOTS_TABLE_NAME;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ColumnsBuilder;
import org.folio.dao.util.ValuesBuilder;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
//   <column name="processing_started_date" type="timestamp"></column>
// </createTable>
@Component
public class LBSnapshotDaoImpl implements LBSnapshotDao {

  private static final Logger LOG = LoggerFactory.getLogger(LBSnapshotDaoImpl.class);

  public static final String PROCESSING_STARTED_DATE_COLUMN_NAME = "processing_started_date";
  public static final String STATUS_COLUMN_NAME = "status";

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
  public String toColumns(Snapshot snapshot) {
    return ColumnsBuilder.of(ID_COLUMN_NAME)
      .append(snapshot.getProcessingStartedDate(), PROCESSING_STARTED_DATE_COLUMN_NAME)
      .append(snapshot.getStatus(), STATUS_COLUMN_NAME)
      .build();
  }

  @Override
  public String toValues(Snapshot snapshot, boolean generateIdIfNotExists) {
    if (generateIdIfNotExists && StringUtils.isEmpty(snapshot.getJobExecutionId())) {
      snapshot.setJobExecutionId(UUID.randomUUID().toString());
    }
    ValuesBuilder valuesBuilder = ValuesBuilder.of(snapshot.getJobExecutionId())
      .append(snapshot.getProcessingStartedDate());
    if (snapshot.getStatus() != null) {
      valuesBuilder
        .append(snapshot.getStatus().toString());
    }
    return valuesBuilder.build();
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
    String processingStartedDate = result.getString(PROCESSING_STARTED_DATE_COLUMN_NAME);
    if (StringUtils.isNotEmpty(processingStartedDate)) {
      try {
        snapshot.setProcessingStartedDate(new SimpleDateFormat(DATE_FORMAT_STR_ISO8601).parse(processingStartedDate));
      } catch (ParseException e) {
        LOG.error(e.getMessage(), e.getCause());
      }
    }
    return snapshot;
  }

}