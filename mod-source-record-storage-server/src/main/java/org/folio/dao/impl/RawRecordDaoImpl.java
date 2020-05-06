package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RAW_RECORDS_TABLE_NAME;

import java.util.stream.Collectors;

import org.folio.dao.AbstractEntityDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.filter.RawRecordFilter;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
public class RawRecordDaoImpl extends AbstractEntityDao<RawRecord, RawRecordCollection, RawRecordFilter> implements RawRecordDao {

  @Override
  public String getTableName() {
    return RAW_RECORDS_TABLE_NAME;
  }

  @Override
  public String getColumns() {
    return ColumnBuilder.of(ID_COLUMN_NAME)
      .append(CONTENT_COLUMN_NAME)
      .build();
  }

  @Override
  public String getId(RawRecord rawRecord) {
    return rawRecord.getId();
  }

  @Override
  protected JsonArray toParams(RawRecord rawRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // error_records id is foreign key with records_lb
    return new JsonArray()
      .add(rawRecord.getId())
      .add(rawRecord.getContent());
  }

  @Override
  protected RawRecordCollection toCollection(ResultSet resultSet) {
    return new RawRecordCollection()
      .withRawRecords(resultSet.getRows().stream().map(this::toEntity).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  @Override
  protected RawRecord toEntity(JsonObject result) {
    return new RawRecord()
      .withId(result.getString(ID_COLUMN_NAME))
      .withContent(result.getString(CONTENT_COLUMN_NAME));
  }

  @Override
  protected RawRecord toEntity(JsonArray row) {
    return new RawRecord()
      .withId(row.getString(0))
      .withContent(row.getString(1));
  }

}