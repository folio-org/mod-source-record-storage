package org.folio.dao.impl;

import java.io.IOException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

  private static final String TABLE_NAME = "marc_records_lb";

  private static final ObjectMapper objectMapper = new ObjectMapper();;

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
    return TABLE_NAME;
  }

  @Override
  public String getId(ParsedRecord parsedRecord) {
    return parsedRecord.getId();
  }

  @Override
  public String toColumns(ParsedRecord parsedRecord) {
    String columns = StringUtils.isNotEmpty(parsedRecord.getId()) ? "id" : StringUtils.EMPTY;
    if (parsedRecord.getContent() != null) {
      columns += columns.length() > 0 ? ",content" : "content";
    }
    return columns;
  }

  @Override
  public String toValues(ParsedRecord parsedRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // parsed_records id if foreign key with records_lb
    String values = StringUtils.isNotEmpty(parsedRecord.getId())
      ? String.format("'%s'", parsedRecord.getId())
      : StringUtils.EMPTY;
    if (parsedRecord.getContent() != null) {
      values = values.length() > 0
        ? String.format("%s,'%s'", values, parsedRecord.getContent())
        : String.format("'%s'", values, parsedRecord.getContent());
    }
    return values;
  }

  @Override
  public ParsedRecordCollection toCollection(ResultSet resultSet) {
    return new ParsedRecordCollection()
        .withParsedRecords(resultSet.getRows().stream().map(this::toBean).collect(Collectors.toList()))
        .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  public ParsedRecord toBean(JsonObject result) {
    String content = result.getString("content");
    ParsedRecord parsedObject = new ParsedRecord()
      .withId(result.getString("id"))
      .withContent(content)
      .withFormattedContent(new JsonObject(content).encodePrettily());
    return parsedObject;
  }

}