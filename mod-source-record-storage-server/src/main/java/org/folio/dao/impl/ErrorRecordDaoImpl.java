package org.folio.dao.impl;

import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

  private static final String TABLE_NAME = "error_records_lb";

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
    return TABLE_NAME;
  }

  @Override
  public String getId(ErrorRecord errorRecord) {
    return errorRecord.getId();
  }

  @Override
  public String toColumns(ErrorRecord errorRecord) {
    String columns = StringUtils.isNotEmpty(errorRecord.getId()) ? "id" : StringUtils.EMPTY;
    if (errorRecord.getContent() != null) {
      columns += columns.length() > 0 ? ",content" : "content";
    }
    if (StringUtils.isNotEmpty(errorRecord.getDescription())) {
      columns += columns.length() > 0 ? ",description" : "description";
    }
    return columns;
  }

  @Override
  public String toValues(ErrorRecord errorRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // error_records id if foreign key with records_lb
    String values = StringUtils.isNotEmpty(errorRecord.getId())
      ? String.format("'%s'", errorRecord.getId())
      : StringUtils.EMPTY;
    if (errorRecord.getContent() != null) {
      values = values.length() > 0
        ? String.format("%s,'%s'", values, errorRecord.getContent())
        : String.format("'%s'", values, errorRecord.getContent());
    }
    if (StringUtils.isNotEmpty(errorRecord.getDescription())) {
      values = values.length() > 0
        ? String.format("%s,'%s'", values, errorRecord.getDescription())
        : String.format("'%s'", values, errorRecord.getDescription());
    }
    return values;
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
      .withId(result.getString("id"))
      .withContent(result.getString("content"))
      .withDescription(result.getString("description"));
  }

}