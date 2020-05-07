package org.folio.dao.impl;

import static java.util.stream.StreamSupport.stream;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.PARSED_RECORDS_TABLE_NAME;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.dao.AbstractEntityDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.filter.ParsedRecordFilter;
import org.folio.dao.util.ColumnBuilder;
import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.springframework.stereotype.Component;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

// <createTable tableName="marc_records_lb">
//   <column name="id" type="uuid">
//     <constraints primaryKey="true" nullable="false"/>
//   </column>
//   <column name="content" type="jsonb">
//     <constraints nullable="false"/>
//   </column>
// </createTable>
@Component
public class ParsedRecordDaoImpl extends AbstractEntityDao<ParsedRecord, ParsedRecordCollection, ParsedRecordFilter> implements ParsedRecordDao {

  @Override
  public String getTableName() {
    return PARSED_RECORDS_TABLE_NAME;
  }

  @Override
  public String getColumns() {
    return ColumnBuilder
      .of(ID_COLUMN_NAME)
      .append(CONTENT_COLUMN_NAME)
      .build();
  }

  @Override
  public String getId(ParsedRecord parsedRecord) {
    return parsedRecord.getId();
  }

  @Override
  protected Tuple toTuple(ParsedRecord parsedRecord, boolean generateIdIfNotExists) {
    // NOTE: ignoring generateIdIfNotExists, id is required
    // parsed_records id is foreign key with records_lb
    Tuple tuple = Tuple.tuple();
    if (Objects.nonNull(parsedRecord.getId())) {
      tuple.addUUID(UUID.fromString(parsedRecord.getId()));
    } else {
      tuple.addValue(null);
    }
    tuple.addValue(parsedRecord.getContent());
    return tuple;
  }

  @Override
  protected ParsedRecordCollection toCollection(RowSet<Row> rowSet) {
    return new ParsedRecordCollection()
      .withParsedRecords(stream(rowSet.spliterator(), false)
        .map(this::toEntity).collect(Collectors.toList()))
      .withTotalRecords(rowSet.rowCount());
  }

  @Override
  protected ParsedRecord toEntity(Row row) {
    String content = row.getString(CONTENT_COLUMN_NAME);
    return formatContent(new ParsedRecord()
      .withId(row.getUUID(ID_COLUMN_NAME).toString())
      .withContent(content));
  }

  @Override
  protected ParsedRecord postSave(ParsedRecord parsedRecord) {
    return formatContent(parsedRecord);
  }

  @Override
  protected ParsedRecord postUpdate(ParsedRecord parsedRecord) {
    return formatContent(parsedRecord);
  }

  private ParsedRecord formatContent(ParsedRecord parsedRecord) {
    try {
      String formattedContent = MarcUtil.marcJsonToTxtMarc((String) parsedRecord.getContent());
      parsedRecord.withFormattedContent(formattedContent);
    } catch (IOException e) {
      log.error("Error formatting content", e);
    }
    return parsedRecord;
  }

}