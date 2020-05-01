package org.folio.dao.impl;

import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.RawRecordDao;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

// <createTable tableName="raw_records_lb">
//   <column name="id" type="uuid">
//     <constraints primaryKey="true" nullable="false"/>
//   </column>
//   <column name="content" type="text">
//     <constraints nullable="false"/>
//   </column>
// </createTable>
@Component
public class RawRecordDaoImpl implements RawRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(RawRecordDaoImpl.class);

  private static final String TABLE_NAME = "raw_records_lb";

  private final PostgresClientFactory pgClientFactory;

  @Autowired 
  public RawRecordDaoImpl(PostgresClientFactory pgClientFactory) {
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
  public String getId(RawRecord rawRecord) {
    return rawRecord.getId();
  }

  @Override
  public String toColumns(RawRecord rawRecord) {
    String columns = StringUtils.isNotEmpty(rawRecord.getId()) ? "id" : StringUtils.EMPTY;
    if (rawRecord.getContent() != null) {
      columns += columns.length() > 0 ? ",content" : "content";
    }
    return columns;
  }

  @Override
  public String toValues(RawRecord rawRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // raw_records id if foreign key with records_lb
    String values = StringUtils.isNotEmpty(rawRecord.getId())
      ? String.format("'%s'", rawRecord.getId())
      : StringUtils.EMPTY;
    if (rawRecord.getContent() != null) {
      values = values.length() > 0
        ? String.format("%s,'%s'", values, rawRecord.getContent())
        : String.format("'%s'", values, rawRecord.getContent());
    }
    return values;
  }

  @Override
  public RawRecordCollection toCollection(ResultSet resultSet) {
    return new RawRecordCollection()
      .withRawRecords(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  public RawRecord toBean(JsonObject result) {
    return new RawRecord()
      .withId(result.getString("id"))
      .withContent(result.getString("content"));
  }

}