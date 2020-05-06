package org.folio.dao.impl;

import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.ORDER_IN_FILE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.*;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_DATE_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.*;
import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;
import static org.folio.dao.util.DaoUtil.GET_BY_FILTER_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.JSON_COLUMN_NAME;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.dao.filter.RecordFilter;
import org.folio.dao.util.MarcUtil;
import org.folio.dao.util.SourceRecordContent;
import org.folio.rest.jaxrs.model.Metadata;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLRowStream;

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
    return select(GET_SOURCE_MARC_RECORD_BY_ID_TEMPLATE, id, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByIdAlt(String id, String tenantId) {
    return select(GET_SOURCE_MARC_RECORD_BY_ID_ALT_TEMPLATE, id, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(String instanceId, String tenantId) {
    return select(GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_TEMPLATE, instanceId, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceIdAlt(String instanceId, String tenantId) {
    return select(GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_ALT_TEMPLATE, instanceId, tenantId);
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
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date till, Integer offset,
      Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_FOR_PERIOD_TEMPLATE, from, till, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date till, Integer offset,
      Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_FOR_PERIOD_ALT_TEMPLATE, from, till, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordById(SourceRecordContent content, String id,
      String tenantId) {
    return recordDao.getById(id, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByMatchedId(SourceRecordContent content, String matchedId,
      String tenantId) {
    return recordDao.getByMatchedId(matchedId, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(SourceRecordContent content, String instanceId,
      String tenantId) {
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
    postgresClientFactory.createInstance(tenantId).getClient().getConnection(connection -> {
      if (connection.failed()) {
        LOG.error("Failed to get database connection", connection.cause());
        endHandler.handle(Future.failedFuture(connection.cause()));
        return;
      }
      connection.result().queryStream(sql, stream -> {
        if (stream.failed()) {
          LOG.error("Failed to get stream", stream.cause());
          endHandler.handle(Future.failedFuture(stream.cause()));
          return;
        }
        stream.result()
          .handler(row -> {
            stream.result().pause();
            lookupContent(content, tenantId, toSourceRecord(row)).setHandler(res -> {
              if (res.failed()) {
                endHandler.handle(Future.failedFuture(res.cause()));
                return;
              }
              handler.handle(res.result());
              stream.result().resume();
            });
          })
          .exceptionHandler(e -> endHandler.handle(Future.failedFuture(e)))
          .endHandler(x -> endHandler.handle(Future.succeededFuture()));
      });
    });
  }

  private Future<Optional<SourceRecord>> select(String template, String id, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(template, id);
    LOG.info("Attempting get source records: {}", sql);
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toSourceRecord);
  }

  private Future<SourceRecordCollection> select(String template, Integer offset, Integer limit, String tenantId) {
    String sql = String.format(template, offset, limit);
    return select(sql, tenantId);
  }

  private Future<SourceRecordCollection> select(String template, Date from, Date till, Integer offset, Integer limit,
      String tenantId) {
    String sql = String.format(template, DATE_FORMATTER.format(from), DATE_FORMATTER.format(till), offset, limit);
    return select(sql, tenantId);
  }

  private Future<SourceRecordCollection> select(String sql, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    LOG.info("Attempting get by source records: {}", sql);
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toSourceRecordCollection);
  }

  private Optional<SourceRecord> toSourceRecord(ResultSet resultSet) {
    return resultSet.getNumRows() > 0 ? Optional.of(toSourceRecord(resultSet.getRows().get(0))) : Optional.empty();
  }

  private SourceRecordCollection toSourceRecordCollection(ResultSet resultSet) {
    return new SourceRecordCollection()
      .withSourceRecords(resultSet.getRows().stream().map(this::toSourceRecord).collect(Collectors.toList()))
      .withTotalRecords(resultSet.getNumRows());
  }

  private SourceRecord toSourceRecord(JsonObject jsonObject) {
    String id = jsonObject.getString(ID_COLUMN_NAME);
    JsonObject jsonb = new JsonObject(jsonObject.getString(JSON_COLUMN_NAME));
    JsonObject content = jsonb.getJsonObject(CONTENT_COLUMN_NAME);
    ParsedRecord parsedRecord = new ParsedRecord()
      .withId(jsonb.getString(ID_COLUMN_NAME))
      .withContent(content.encode());
    try {
      String formattedContent = MarcUtil.marcJsonToTxtMarc(content.encode());
      parsedRecord.withFormattedContent(formattedContent);
    } catch (IOException e) {
      LOG.error("Error formatting content", e);
    }
    // NOTE: will be missing several properties; record type, snapshot id, etc.
    return new SourceRecord()
      .withRecordId(id)
      .withParsedRecord(parsedRecord);
  }

  private SourceRecord toSourceRecord(JsonArray row) {
    Metadata metadata = new Metadata();
    String createdByUserId = row.getString(4);
    if (StringUtils.isNotEmpty(createdByUserId)) {
      metadata.setCreatedByUserId(createdByUserId);
    }
    Instant createdDate = row.getInstant(5);
    if (Objects.nonNull(createdDate)) {
      metadata.setCreatedDate(Date.from(createdDate));
    }
    String updatedByUserId = row.getString(6);
    if (StringUtils.isNotEmpty(updatedByUserId)) {
      metadata.setUpdatedByUserId(updatedByUserId);
    }
    Instant updatedDate = row.getInstant(7);
    if (Objects.nonNull(updatedDate)) {
      metadata.setUpdatedDate(Date.from(updatedDate));
    }
    return new SourceRecord()
      .withRecordId(row.getString(0))
      .withSnapshotId(row.getString(1))
      .withOrder(row.getInteger(2))
      // NOTE: not ideal to have multiple record type enums
      .withRecordType(RecordType.fromValue(row.getString(3)))
      .withMetadata(metadata);
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
    ).setHandler(lookup -> {
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
        ).setHandler(lookup -> promise.complete(sourceRecord));
        break;
      case PARSED_RECORD_ONLY:
        parsedRecordDao.getById(id, tenantId).map(parsedRecord -> addParsedRecordContent(sourceRecord, parsedRecord))
          .setHandler(lookup -> promise.complete(sourceRecord));
        break;
      case RAW_RECORD_ONLY:
        rawRecordDao.getById(id, tenantId).map(rawRecord -> addRawRecordContent(sourceRecord, rawRecord))
          .setHandler(lookup -> promise.complete(sourceRecord));
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