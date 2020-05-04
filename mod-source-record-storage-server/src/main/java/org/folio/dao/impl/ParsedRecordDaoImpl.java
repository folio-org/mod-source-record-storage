package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.PARSED_RECORDS_TABLE_NAME;

import java.util.stream.Collectors;

import org.folio.dao.ParsedRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

// <createTable tableName="marc_records_lb">
//   <column name="id" type="uuid">
//     <constraints primaryKey="true" nullable="false"/>
//   </column>
//   <column name="content" type="jsonb">
//     <constraints nullable="false"/>
//   </column>
// </createTable>
@Component
public class ParsedRecordDaoImpl implements ParsedRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(ParsedRecordDaoImpl.class);

  private final PostgresClientFactory pgClientFactory;

  @Autowired
  public ParsedRecordDaoImpl(PostgresClientFactory pgClientFactory) {
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
    return PARSED_RECORDS_TABLE_NAME;
  }

  @Override
  public String getId(ParsedRecord parsedRecord) {
    return parsedRecord.getId();
  }

  @Override
  public String getColumns() {
    return ColumnBuilder.of(ID_COLUMN_NAME)
      .append(CONTENT_COLUMN_NAME)
      .build();
  }

  @Override
  public JsonArray toParams(ParsedRecord parsedRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // error_records id is foreign key with records_lb
    return new JsonArray()
      .add(parsedRecord.getId())
      .add(parsedRecord.getContent());
  }

  @Override
  public ParsedRecordCollection toCollection(ResultSet resultSet) {
    return new ParsedRecordCollection()
      .withParsedRecords(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  public ParsedRecord toBean(JsonObject result) {
    String content = result.getString(CONTENT_COLUMN_NAME);
    // TODO: handle formatted content
    // could add record type to function response
    // then pass content and type to utility to convert to formatted content
    return new ParsedRecord()
      .withId(result.getString(ID_COLUMN_NAME))
      .withContent(content);
  }

}