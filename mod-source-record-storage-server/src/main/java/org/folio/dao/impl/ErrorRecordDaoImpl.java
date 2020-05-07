package org.folio.dao.impl;

import static java.util.stream.StreamSupport.stream;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ERROR_RECORDS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.dao.AbstractEntityDao;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.dao.util.ColumnBuilder;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

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
  protected Tuple toTuple(ErrorRecord errorRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // raw_records id is foreign key with records_lb
    Tuple tuple = Tuple.tuple();
    if (Objects.nonNull(errorRecord.getId())) {
      tuple.addUUID(UUID.fromString(errorRecord.getId()));
    } else {
      tuple.addValue(null);
    }
    tuple
      .addValue(errorRecord.getContent())
      .addString(errorRecord.getDescription());
    return tuple;
  }

  @Override
  protected ErrorRecordCollection toCollection(RowSet<Row> rowSet) {
    return new ErrorRecordCollection()
      .withErrorRecords(stream(rowSet.spliterator(), false)
        .map(this::toEntity).collect(Collectors.toList()))
      .withTotalRecords(rowSet.rowCount());
  }

  @Override
  protected ErrorRecord toEntity(Row row) {
    return new ErrorRecord()
      .withId(row.getUUID(ID_COLUMN_NAME).toString())
      .withContent(row.getString(CONTENT_COLUMN_NAME))
      .withDescription(row.getString(DESCRIPTION_COLUMN_NAME));
  }

}