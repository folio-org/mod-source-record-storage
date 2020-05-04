package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ERROR_RECORDS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.stream.Collectors;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

// <createTable tableName="error_records_lb">
//   <column name="id" type="uuid">
//     <constraints primaryKey="true" nullable="false"/>
//   </column>
//   <column name="content" type="jsonb">
//     <constraints nullable="false"/>
//   </column>
//   <column name="description" type="varchar(1024)">
//     <constraints nullable="false"/>
//   </column>
// </createTable>
@Component
public class ErrorRecordDaoImpl implements ErrorRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorRecordDaoImpl.class);

  public static final String DESCRIPTION_COLUMN_NAME = "description";

  private final PostgresClientFactory pgClientFactory;

  @Autowired 
  public ErrorRecordDaoImpl(PostgresClientFactory pgClientFactory) {
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
    return ERROR_RECORDS_TABLE_NAME;
  }

  @Override
  public String getId(ErrorRecord errorRecord) {
    return errorRecord.getId();
  }

  @Override
  public String getColumns() {
    return ColumnBuilder
      .of(ID_COLUMN_NAME)
      .append(CONTENT_COLUMN_NAME)
      .append(DESCRIPTION_COLUMN_NAME)
      .build();
  }

  @Override
  public JsonArray toParams(ErrorRecord errorRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // error_records id is foreign key with records_lb
    return new JsonArray()
      .add(errorRecord.getId())
      .add(errorRecord.getContent())
      .add(errorRecord.getDescription());
  }

  @Override
  public ErrorRecordCollection toCollection(ResultSet resultSet) {
    return new ErrorRecordCollection()
      .withErrorRecords(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  public ErrorRecord toBean(JsonObject result) {
    return new ErrorRecord()
      .withId(result.getString(ID_COLUMN_NAME))
      .withContent(result.getString(CONTENT_COLUMN_NAME))
      .withDescription(result.getString(DESCRIPTION_COLUMN_NAME));
  }

}