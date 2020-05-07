package org.folio.dao.impl;

import static java.util.stream.StreamSupport.stream;
import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.ORDER_IN_FILE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.RECORD_TYPE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SNAPSHOT_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_DATE_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;
import static org.folio.dao.util.DaoUtil.GET_BY_FILTER_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.JSONB_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.RECORDS_TABLE_NAME;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.dao.filter.RecordFilter;
import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.MarcUtil;
import org.folio.dao.util.SourceRecordContent;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecord.RecordType;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

@Component
public class SourceRecordDaoImpl implements SourceRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRecordDaoImpl.class);

  private static final String SOURCE_RECORD_COLUMNS = String.join(COMMA, ID_COLUMN_NAME, SNAPSHOT_ID_COLUMN_NAME, ORDER_IN_FILE_COLUMN_NAME, RECORD_TYPE_COLUMN_NAME,
    CREATED_BY_USER_ID_COLUMN_NAME, CREATED_DATE_COLUMN_NAME, UPDATED_BY_USER_ID_COLUMN_NAME, UPDATED_DATE_COLUMN_NAME);

  private static final String GET_SOURCE_MARC_RECORD_BY_ID_TEMPLATE = "SELECT * FROM get_source_marc_record_by_id('%s') as records;";
  private static final String GET_SOURCE_MARC_RECORD_BY_ID_ALT_TEMPLATE = "SELECT * FROM get_source_marc_record_by_id_alt('%s') as records;";

  private static final String GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_TEMPLATE = "SELECT * FROM get_source_marc_record_by_instance_id('%s') as records;";
  private static final String GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_ALT_TEMPLATE = "SELECT * FROM get_source_marc_record_by_instance_id_alt('%s') as records;";

  private static final String GET_SOURCE_MARC_RECORDS_TEMPLATE = "SELECT * FROM get_all_source_marc_records(%s,%s) as records;";
  private static final String GET_SOURCE_MARC_RECORDS_ALT_TEMPLATE = "SELECT * FROM get_all_source_marc_records_alt(%s,%s) as records;";

  private static final String GET_SOURCE_MARC_RECORDS_FOR_PERIOD_TEMPLATE = "SELECT * FROM get_source_marc_records_for_period('%s','%s',%s,%s) as records;";
  private static final String GET_SOURCE_MARC_RECORDS_FOR_PERIOD_ALT_TEMPLATE = "SELECT * FROM get_source_marc_records_for_period_alt('%s','%s',%s,%s) as records;";

  @Autowired
  private PostgresClientFactory postgresClientFactory;

  @Autowired
  private LBRecordDao recordDao;

  @Autowired
  private RawRecordDao rawRecordDao;

  @Autowired
  private ParsedRecordDao parsedRecordDao;

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordById(String id, String tenantId) {
    return selectById(GET_SOURCE_MARC_RECORD_BY_ID_TEMPLATE, id, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByIdAlt(String id, String tenantId) {
    return selectById(GET_SOURCE_MARC_RECORD_BY_ID_ALT_TEMPLATE, id, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(String instanceId, String tenantId) {
    return selectById(GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_TEMPLATE, instanceId, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceIdAlt(String instanceId, String tenantId) {
    return selectById(GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_ALT_TEMPLATE, instanceId, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecords(Integer offset, Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_TEMPLATE, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsAlt(Integer offset, Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_ALT_TEMPLATE, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date till, Integer offset, Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_FOR_PERIOD_TEMPLATE, from, till, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date till, Integer offset, Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_FOR_PERIOD_ALT_TEMPLATE, from, till, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordById(SourceRecordContent content, String id, String tenantId) {
    return recordDao.getById(id, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByMatchedId(SourceRecordContent content, String matchedId, String tenantId) {
    return recordDao.getByMatchedId(matchedId, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(SourceRecordContent content, String instanceId, String tenantId) {
    return recordDao.getByInstanceId(instanceId, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsByFilter(SourceRecordContent content, RecordFilter filter, Integer offset,
      Integer limit, String tenantId) {
    return recordDao.getByFilter(filter, offset, limit, tenantId)
      .compose(recordCollection -> lookupContent(content, tenantId, recordCollection));
  }

  @Override
  public void getSourceMarcRecordsByFilter(SourceRecordContent content, RecordFilter filter, Integer offset, Integer limit, String tenantId,
    Handler<SourceRecord> handler, Handler<AsyncResult<Void>> endHandler) {
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, SOURCE_RECORD_COLUMNS, RECORDS_TABLE_NAME, filter.toWhereClause(), offset, limit);
    LOG.info("Attempting stream get by filter: {}", sql);
    postgresClientFactory.getClient(tenantId).getConnection(ar1 -> {
      if (ar1.failed()) {
        LOG.error("Failed to get database connection", ar1.cause());
        endHandler.handle(Future.failedFuture(ar1.cause()));
        return;
      }
      SqlConnection connection = ar1.result();
      connection.prepare(sql, ar2 -> {
        if (ar2.failed()) {
          LOG.error("Failed to prepare query", ar2.cause());
          endHandler.handle(Future.failedFuture(ar2.cause()));
          return;
        }
        PreparedStatement pq = ar2.result();
        Transaction tx = connection.begin();
        RowStream<Row> stream = pq.createStream(limit, Tuple.tuple());
        stream
          .handler(row -> {
            stream.pause();
            lookupContent(content, tenantId, toSourceRecord(row)).onComplete(ar3 -> {
              if (ar3.failed()) {
                endHandler.handle(Future.failedFuture(ar3.cause()));
                return;
              }
              handler.handle(ar3.result());
              stream.resume();
            });
          })
          .exceptionHandler(e -> endHandler.handle(Future.failedFuture(e)))
          .endHandler(x -> {
            tx.commit();
            endHandler.handle(Future.succeededFuture());
          });
      });
    });
  }

  private Future<Optional<SourceRecord>> selectById(String template, String id, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String sql = String.format(template, id);
    LOG.info("Attempting get source records: {}", sql);
    postgresClientFactory.getClient(tenantId).query(sql).execute(promise);
    return promise.future().map(this::toPartialSourceRecord);
  }

  private Future<SourceRecordCollection> select(String template, Integer offset, Integer limit, String tenantId) {
    String sql = String.format(template, offset, limit);
    return select(sql, tenantId);
  }

  private Future<SourceRecordCollection> select(String template, Date from, Date till, Integer offset, Integer limit, String tenantId) {
    String sql = String.format(template, DATE_FORMATTER.format(from), DATE_FORMATTER.format(till), offset, limit);
    return select(sql, tenantId);
  }

  private Future<SourceRecordCollection> select(String sql, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    LOG.info("Attempting get source records: {}", sql);
    postgresClientFactory.getClient(tenantId).query(sql).execute(promise);
    return promise.future().map(this::toPartialSourceRecordCollection);
  }

  private Optional<SourceRecord> toPartialSourceRecord(RowSet<Row> rowSet) {
    return rowSet.rowCount() > 0 ? Optional.of(toPartialSourceRecord(rowSet.iterator().next())) : Optional.empty();
  }

  private SourceRecordCollection toPartialSourceRecordCollection(RowSet<Row> rowSet) {
    return new SourceRecordCollection()
      .withSourceRecords(stream(rowSet.spliterator(), false)
        .map(this::toPartialSourceRecord).collect(Collectors.toList()))
      .withTotalRecords(rowSet.rowCount());
  }

  private SourceRecord toPartialSourceRecord(Row row) {
    ParsedRecord parsedRecord = new ParsedRecord();
    Object jsonb = row.getValue(JSONB_COLUMN_NAME);
    if (Objects.nonNull(jsonb)) {
      JsonObject json = (JsonObject) jsonb;
      String content = json.getString(CONTENT_COLUMN_NAME);
      parsedRecord
        .withId(json.getString(ID_COLUMN_NAME))
        .withContent(content);
      try {
        String formattedContent = MarcUtil.marcJsonToTxtMarc(content);
        parsedRecord.withFormattedContent(formattedContent);
      } catch (IOException e) {
        LOG.error("Error formatting content", e);
      }
    }
    return toMinimumSourceRecord(row)
      .withParsedRecord(parsedRecord);
  }

  private SourceRecord toSourceRecord(Row row) {
    return toMinimumSourceRecord(row)
      .withSnapshotId(row.getUUID(SNAPSHOT_ID_COLUMN_NAME).toString())
      .withOrder(row.getInteger(ORDER_IN_FILE_COLUMN_NAME))
      .withRecordType(RecordType.fromValue(row.getString(RECORD_TYPE_COLUMN_NAME)))
      .withMetadata(DaoUtil.metadataFromRow(row, new String[] {
        CREATED_BY_USER_ID_COLUMN_NAME,
        CREATED_DATE_COLUMN_NAME,
        UPDATED_BY_USER_ID_COLUMN_NAME,
        UPDATED_DATE_COLUMN_NAME
      }));
  }

  private SourceRecord toMinimumSourceRecord(Row row) {
    SourceRecord sourceRecord = new SourceRecord();
    UUID id = row.getUUID(ID_COLUMN_NAME);
    if (Objects.nonNull(id)) {
      sourceRecord.withRecordId(id.toString());
    }
    return sourceRecord;
  }

  private Optional<SourceRecord> toSourceRecord(Optional<Record> record) {
    if (record.isPresent()) {
      return Optional.of(toSourceRecord(record.get()));
    }
    return Optional.empty();
  }

  private SourceRecord toSourceRecord(Record record) {
    return new SourceRecord()
      .withRecordId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withOrder(record.getOrder())
      // NOTE: not ideal to have multiple record type enums
      .withRecordType(RecordType.fromValue(record.getRecordType().toString()))
      .withMetadata(record.getMetadata());
  }

  private Future<SourceRecordCollection> lookupContent(SourceRecordContent content, String tenantId, RecordCollection recordCollection) {
    Promise<SourceRecordCollection> promise = Promise.promise();
    CompositeFuture.all(
      recordCollection.getRecords().stream()
        .map(this::toSourceRecord)
        .map(sr -> lookupContent(content, tenantId, sr))
        .collect(Collectors.toList())
    ).onComplete(lookup -> {
      List<SourceRecord> sourceRecords = lookup.result().list();
      promise.complete(new SourceRecordCollection()
        .withSourceRecords(sourceRecords)
        .withTotalRecords(sourceRecords.size()));
    });
    return promise.future();
  }

  private Future<Optional<SourceRecord>> lookupContent(SourceRecordContent content, String tenantId, Optional<SourceRecord> sourceRecord) {
    if (sourceRecord.isPresent()) {
      return lookupContent(content, tenantId, sourceRecord.get()).map(Optional::of);
    }
    return Future.factory.succeededFuture(sourceRecord);
  }

  private Future<SourceRecord> lookupContent(SourceRecordContent content, String tenantId, SourceRecord sourceRecord) {
    String id = sourceRecord.getRecordId();
    Promise<SourceRecord> promise = Promise.promise();
    switch(content) {
      case RAW_AND_PARSED_RECORD:
        CompositeFuture.all(
          rawRecordDao.getById(id, tenantId).map(rawRecord -> addRawRecordContent(sourceRecord, rawRecord)),
          parsedRecordDao.getById(id, tenantId).map(parsedRecord -> addParsedRecordContent(sourceRecord, parsedRecord))
        ).onComplete(lookup -> promise.complete(sourceRecord));
        break;
      case PARSED_RECORD_ONLY:
        parsedRecordDao.getById(id, tenantId).map(parsedRecord -> addParsedRecordContent(sourceRecord, parsedRecord))
          .onComplete(lookup -> promise.complete(sourceRecord));
        break;
      case RAW_RECORD_ONLY:
        rawRecordDao.getById(id, tenantId).map(rawRecord -> addRawRecordContent(sourceRecord, rawRecord))
          .onComplete(lookup -> promise.complete(sourceRecord));
        break;
    }
    return promise.future();
  }

  private SourceRecord addRawRecordContent(SourceRecord sourceRecord, Optional<RawRecord> rawRecord) {
    if (rawRecord.isPresent()) {
      sourceRecord.withRawRecord(rawRecord.get());
    }
    return sourceRecord;
  }

  private SourceRecord addParsedRecordContent(SourceRecord sourceRecord, Optional<ParsedRecord> parsedRecord) {
    if (parsedRecord.isPresent()) {
      sourceRecord.withParsedRecord(parsedRecord.get());
    }
    return sourceRecord;
  }

}