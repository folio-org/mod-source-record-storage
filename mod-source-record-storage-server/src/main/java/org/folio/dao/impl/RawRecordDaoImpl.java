package org.folio.dao.impl;

import static java.util.stream.StreamSupport.stream;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RAW_RECORDS_TABLE_NAME;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.dao.AbstractEntityDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.filter.RawRecordFilter;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

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
    return ColumnBuilder
      .of(ID_COLUMN_NAME)
      .append(CONTENT_COLUMN_NAME)
      .build();
  }

  @Override
  public String getId(RawRecord rawRecord) {
    return rawRecord.getId();
  }

  @Override
  protected Tuple toTuple(RawRecord rawRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // raw_records id is foreign key with records_lb
    Tuple tuple = Tuple.tuple();
    if (Objects.nonNull(rawRecord.getId())) {
      tuple.addUUID(UUID.fromString(rawRecord.getId()));
    } else {
      tuple.addValue(null);
    }
    tuple.addValue(rawRecord.getContent());
    return tuple;
  }

  @Override
  protected RawRecordCollection toCollection(RowSet<Row> rowSet) {
    return new RawRecordCollection()
      .withRawRecords(stream(rowSet.spliterator(), false)
        .map(this::toEntity).collect(Collectors.toList()))
      .withTotalRecords(rowSet.rowCount());
  }

  @Override
  protected RawRecord toEntity(Row row) {
    return new RawRecord()
      .withId(row.getUUID(ID_COLUMN_NAME).toString())
      .withContent(row.getString(CONTENT_COLUMN_NAME));
  }

}