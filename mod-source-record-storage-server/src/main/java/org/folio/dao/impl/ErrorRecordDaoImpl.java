package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ERROR_RECORDS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.stream.Collectors;

import org.folio.dao.AbstractEntityDao;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
public class ErrorRecordDaoImpl extends AbstractEntityDao<ErrorRecord, ErrorRecordCollection, ErrorRecordFilter> implements ErrorRecordDao {

  public static final String DESCRIPTION_COLUMN_NAME = "description";

  @Override
  public String getTableName() {
    return ERROR_RECORDS_TABLE_NAME;
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
  public String getId(ErrorRecord errorRecord) {
    return errorRecord.getId();
  }

  @Override
  protected JsonArray toParams(ErrorRecord errorRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // error_records id is foreign key with records_lb
    return new JsonArray()
      .add(errorRecord.getId())
      .add(errorRecord.getContent())
      .add(errorRecord.getDescription());
  }

  @Override
  protected ErrorRecordCollection toCollection(ResultSet resultSet) {
    return new ErrorRecordCollection()
      .withErrorRecords(resultSet.getRows().stream().map(this::toEntity).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  protected ErrorRecord toEntity(JsonObject result) {
    return new ErrorRecord()
      .withId(result.getString(ID_COLUMN_NAME))
      .withContent(result.getString(CONTENT_COLUMN_NAME))
      .withDescription(result.getString(DESCRIPTION_COLUMN_NAME));
  }

  @Override
  protected ErrorRecord toEntity(JsonArray row) {
    return new ErrorRecord()
      .withId(row.getString(0))
      .withContent(row.getString(1))
      .withDescription(row.getString(2));
  }

}