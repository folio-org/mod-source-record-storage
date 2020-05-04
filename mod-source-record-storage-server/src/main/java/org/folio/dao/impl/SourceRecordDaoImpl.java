package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.JSON_COLUMN_NAME;

import java.io.IOException;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Collectors;

import org.folio.dao.PostgresClientFactory;
import org.folio.dao.SourceRecordDao;
import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecord.RecordType;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.marc4j.MarcException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;

@Component
public class SourceRecordDaoImpl implements SourceRecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRecordDaoImpl.class);

  private static final String GET_SOURCE_MARC_RECORD_BY_ID_TEMPLATE = "SELECT * FROM get_source_marc_record_by_id('%s') as records;";
  private static final String GET_SOURCE_MARC_RECORD_BY_ID_ALT_TEMPLATE = "SELECT * FROM get_source_marc_record_by_id_alt('%s') as records;";

  private static final String GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_TEMPLATE = "SELECT * FROM get_source_marc_record_by_instance_id('%s') as records;";
  private static final String GET_SOURCE_MARC_RECORD_BY_INSTANCE_ID_ALT_TEMPLATE = "SELECT * FROM get_source_marc_record_by_instance_id_alt('%s') as records;";

  private static final String GET_SOURCE_MARC_RECORDS_TEMPLATE = "SELECT * FROM get_all_source_marc_records(%s,%s) as records;";
  private static final String GET_SOURCE_MARC_RECORDS_ALT_TEMPLATE = "SELECT * FROM get_all_source_marc_records_alt(%s,%s) as records;";

  private static final String GET_SOURCE_MARC_RECORDS_FOR_PERIOD_TEMPLATE = "SELECT * FROM get_source_marc_records_for_period('%s','%s',%s,%s) as records;";
  private static final String GET_SOURCE_MARC_RECORDS_FOR_PERIOD_ALT_TEMPLATE = "SELECT * FROM get_source_marc_records_for_period_alt('%s','%s',%s,%s) as records;";

  private final PostgresClientFactory pgClientFactory;

  @Autowired
  public SourceRecordDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

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
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date to, Integer offset, Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_FOR_PERIOD_TEMPLATE, from, to, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date to, Integer offset, Integer limit, String tenantId) {
    return select(GET_SOURCE_MARC_RECORDS_FOR_PERIOD_ALT_TEMPLATE, from, to, offset, limit, tenantId);
  }

  private Future<Optional<SourceRecord>> select(String template, String id, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(template, id);
    LOG.info("Attempting get source records: {}", sql);
    pgClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toSourceRecord);
  }

  private Future<SourceRecordCollection> select(String template, Integer offset, Integer limit, String tenantId) {
    String sql = String.format(template, offset, limit);
    return select(sql, tenantId);
  }

  private Future<SourceRecordCollection> select(String template, Date from, Date to, Integer offset, Integer limit, String tenantId) {
    String sql = String.format(template, from, to, offset, limit);
    return select(sql, tenantId);
  }

  private Future<SourceRecordCollection> select(String sql, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    LOG.info("Attempting get by source records: {}", sql);
    pgClientFactory.createInstance(tenantId).select(sql, promise);
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
    } catch (MarcException | IOException e) {
      LOG.error("Error formatting content", e);
    }
    // NOTE: will be missing several properties; record type, snapshot id, etc.
    return new SourceRecord()
      .withRecordId(id)
      .withParsedRecord(parsedRecord);
  }



}