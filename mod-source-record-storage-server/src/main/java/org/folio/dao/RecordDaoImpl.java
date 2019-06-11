package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordModel;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;
import static org.folio.rest.persist.PostgresClient.pojo2json;

@Component
public class RecordDaoImpl implements RecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(RecordDaoImpl.class);

  private static final String RECORDS_VIEW = "records_view";
  private static final String SOURCE_RECORDS_VIEW = "source_records_view";
  private static final String RECORDS_TABLE = "records";
  private static final String RAW_RECORDS_TABLE = "raw_records";
  private static final String ERROR_RECORDS_TABLE = "error_records";
  private static final String ID_FIELD = "'id'";
  private static final String CALL_GET_HIGHEST_GENERATION_FUNCTION = "select get_highest_generation('%s', '%s');";
  private static final String UPSERT_QUERY = "INSERT INTO %s.%s (_id, jsonb) VALUES (?, ?) ON CONFLICT (_id) DO UPDATE SET jsonb = ?;";
  private static final String CALL_GET_RECORD_BY_INSTANCE_ID_FUNCTION = "select get_record_by_instance_id('%s');";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId) {
    Future<Results<Record>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(RECORDS_VIEW, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(RECORDS_VIEW, Record.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying records_view", e);
      future.fail(e);
    }
    return future.map(results -> new RecordCollection()
      .withRecords(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    Future<Results<Record>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClientFactory.createInstance(tenantId).get(RECORDS_VIEW, Record.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying records_view by id", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(records -> records.isEmpty() ? Optional.empty() : Optional.of(records.get(0)));
  }

  @Override
  public Future<Boolean> saveRecord(Record record, String tenantId) {
    return insertOrUpdateRecord(record, tenantId);
  }

  @Override
  public Future<Boolean> updateRecord(Record record, String tenantId) {
    return insertOrUpdateRecord(record, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId) {
    Future<Results<SourceRecord>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(SOURCE_RECORDS_VIEW, "deleted=" + deletedRecords, limit, offset);
      cql.addWrapper(getCQLWrapper(SOURCE_RECORDS_VIEW, query));
      pgClientFactory.createInstance(tenantId).get(SOURCE_RECORDS_VIEW, SourceRecord.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying results_view", e);
      future.fail(e);
    }
    return future.map(results -> new SourceRecordCollection()
      .withSourceRecords(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Integer> calculateGeneration(Record record, String tenantId) {
    Future<ResultSet> future = Future.future();
    try {
      String getHighestGeneration = String.format(CALL_GET_HIGHEST_GENERATION_FUNCTION, record.getMatchedId(), record.getSnapshotId());
      pgClientFactory.createInstance(tenantId).select(getHighestGeneration, future.completer());
    } catch (Exception e) {
      LOG.error("Error while searching for records highest generation", e);
      future.fail(e);
    }
    return future.map(resultSet -> {
      Integer generation = resultSet.getResults().get(0).getInteger(0);
      if (generation == null) {
        return 0;
      }
      return ++generation;
    });
  }

  @Override
  public Future<Boolean> updateParsedRecord(ParsedRecord parsedRecord, ParsedRecordCollection.RecordType recordType, String tenantId) {
    Future<Boolean> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, parsedRecord.getId());
      pgClientFactory.createInstance(tenantId).update(RecordType.valueOf(recordType.value()).getTableName(),
        convertParsedRecordToJsonObject(parsedRecord), new Criterion(idCrit), true, updateResult -> {
          if (updateResult.failed()) {
            LOG.error("Could not update ParsedRecord with id {}", updateResult.cause(), parsedRecord.getId());
            future.fail(updateResult.cause());
          } else if (updateResult.result().getUpdated() != 1) {
            String errorMessage = String.format("ParsedRecord with id '%s' was not found", parsedRecord.getId());
            LOG.error(errorMessage);
            future.fail(new NotFoundException(errorMessage));
          } else {
            future.complete(true);
          }
        });
    } catch (Exception e) {
      LOG.error("Error updating ParsedRecord with id {}", e, parsedRecord.getId());
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Optional<Record>> getRecordByInstanceId(String instanceId, String tenantId) {
    Future<ResultSet> future = Future.future();
    try {
      String query = String.format(CALL_GET_RECORD_BY_INSTANCE_ID_FUNCTION, instanceId);
      pgClientFactory.createInstance(tenantId).select(query, future.completer());
    } catch (Exception e) {
      LOG.error("Error while searching for Record by instance id {}", e, instanceId);
      future.fail(e);
    }
    return future.map(resultSet -> {
      String record = resultSet.getResults().get(0).getString(0);
      if (record == null) {
        return Optional.empty();
      }
      return Optional.of(new JsonObject(record).mapTo(Record.class));
    });
  }

  private Future<Boolean> insertOrUpdateRecord(Record record, String tenantId) {
    Future<Boolean> future = Future.future();
    RecordModel recordModel = new RecordModel() //NOSONAR
      .withId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withMatchedProfileId(record.getMatchedProfileId())
      .withMatchedId(record.getMatchedId())
      .withGeneration(record.getGeneration())
      .withRecordType(RecordModel.RecordType.fromValue(record.getRecordType().value()))
      .withRawRecordId(record.getRawRecord().getId())
      .withDeleted(record.getDeleted())
      .withAdditionalInfo(record.getAdditionalInfo())
      .withMetadata(record.getMetadata());
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(tx.completer());
        return tx;
      }).compose(v -> insertOrUpdate(tx, record.getRawRecord(), record.getRawRecord().getId(), RAW_RECORDS_TABLE, tenantId))
      .compose(v -> {
        if (record.getParsedRecord() != null) {
          recordModel.setParsedRecordId(record.getParsedRecord().getId());
          return insertOrUpdateParsedRecord(tx, record, tenantId);
        }
        return Future.succeededFuture();
      })
      .compose(v -> {
        if (record.getErrorRecord() != null) {
          recordModel.setErrorRecordId(record.getErrorRecord().getId());
          return insertOrUpdate(tx, record.getErrorRecord(), record.getErrorRecord().getId(), ERROR_RECORDS_TABLE, tenantId);
        }
        return Future.succeededFuture();
      })
      .compose(v -> insertOrUpdate(tx, recordModel, recordModel.getId(), RECORDS_TABLE, tenantId))
      .setHandler(result -> {
        if (result.succeeded()) {
          pgClientFactory.createInstance(tenantId).endTx(tx, endTx ->
            future.complete(true));
        } else {
          pgClientFactory.createInstance(tenantId).rollbackTx(tx, r -> {
            LOG.error("Failed to insert or update Record with id {}. Rollback transaction", result.cause(), record.getId());
            future.fail(result.cause());
          });
        }
      });
    return future;
  }

  private Future<Boolean> insertOrUpdate(AsyncResult<SQLConnection> tx, Object rawRecord, String id, String tableName, String tenantId) {
    Future<UpdateResult> future = Future.future();
    try {
      JsonArray params = new JsonArray().add(id).add(pojo2json(rawRecord)).add(pojo2json(rawRecord));
      String query = String.format(UPSERT_QUERY, PostgresClient.convertToPsqlStandard(tenantId), tableName);
      pgClientFactory.createInstance(tenantId).execute(tx, query, params, future.completer());
      return future.map(result -> result.getUpdated() == 1);
    } catch (Exception e) {
      LOG.error("Error updating Record", e);
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> insertOrUpdateParsedRecord(AsyncResult<SQLConnection> tx, Record record, String tenantId) {
    Future<UpdateResult> future = Future.future();
    ParsedRecord parsedRecord = record.getParsedRecord();
    try {
      JsonObject jsonData = convertParsedRecordToJsonObject(parsedRecord);
      JsonArray params = new JsonArray().add(
        parsedRecord.getId()).add(pojo2json(jsonData)).add(pojo2json(jsonData));
      String query = String.format(UPSERT_QUERY, PostgresClient.convertToPsqlStandard(tenantId), RecordType.valueOf(record.getRecordType().value()).getTableName());
      record.setParsedRecord(jsonData.mapTo(ParsedRecord.class));
      pgClientFactory.createInstance(tenantId).execute(tx, query, params, future.completer());
      return future.map(result -> result.getUpdated() == 1);
    } catch (Exception e) {
      LOG.error("Error mapping ParsedRecord to JsonObject", e);
      ErrorRecord error = new ErrorRecord()
        .withId(UUID.randomUUID().toString())
        .withDescription(e.getMessage())
        .withContent(parsedRecord.getContent());
      record.setErrorRecord(error);
      record.setParsedRecord(null);
      return Future.succeededFuture(false);
    }
  }

  /**
   * Maps parsedRecord to JsonObject
   *
   * @param parsedRecord parsed record
   * @return JsonObject containing parsed record data
   */
  private JsonObject convertParsedRecordToJsonObject(ParsedRecord parsedRecord) {
    if (parsedRecord.getContent() instanceof String) {
      parsedRecord.setContent(new JsonObject(parsedRecord.getContent().toString()));
    }
    return JsonObject.mapFrom(parsedRecord);
  }

}
