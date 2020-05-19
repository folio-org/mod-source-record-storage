package org.folio.dao.impl;

import static java.util.stream.StreamSupport.stream;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.PARSED_RECORDS_TABLE_NAME;
import static org.folio.dao.util.DaoUtil.executeInTransaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.folio.dao.AbstractEntityDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.dao.util.ColumnBuilder;
import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.MarcUtil;
import org.folio.dao.util.TupleWrapper;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Future;
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
public class ParsedRecordDaoImpl extends AbstractEntityDao<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery> implements ParsedRecordDao {

  @Autowired
  private LBRecordDao recordDao;

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    String id = record.getId();
    ParsedRecord parsedRecord = record.getParsedRecord();
    ExternalIdsHolder externalIdsHolder = record.getExternalIdsHolder();
    Metadata metadata = record.getMetadata();
    // NOTE: not updating raw record or error record from incoming record
    return executeInTransaction(postgresClientFactory.getClient(tenantId), connection -> {
      return recordDao.getById(connection, id, tenantId)
        .map(r -> r.orElseThrow(() -> new NotFoundException(String.format("Couldn't find record with id %s", id))))
        .compose(r -> recordDao.save(connection, r.withExternalIdsHolder(externalIdsHolder).withMetadata(metadata), tenantId))
        .compose(r -> save(connection, parsedRecord, tenantId));
    });
  }

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
    return TupleWrapper.of()
      .addUUID(parsedRecord.getId())
      .addValue(parsedRecord.getContent())
      .get();
  }

  @Override
  protected ParsedRecordCollection toCollection(RowSet<Row> rowSet) {
    return toEmptyCollection(rowSet)
      .withParsedRecords(stream(rowSet.spliterator(), false)
        .map(this::toEntity).collect(Collectors.toList()));
  }

  @Override
  protected ParsedRecordCollection toEmptyCollection(RowSet<Row> rowSet) {
    return new ParsedRecordCollection()
      .withParsedRecords(Collections.emptyList())
      .withTotalRecords(DaoUtil.getTotalRecords(rowSet));
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

  @Override
  protected List<ParsedRecord> postSave(List<ParsedRecord> parsedRecords) {
    return parsedRecords.stream().map(this::postSave).collect(Collectors.toList());
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