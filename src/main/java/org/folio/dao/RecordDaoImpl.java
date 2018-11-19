package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordModel;
import org.folio.rest.jaxrs.model.Result;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.folio.dao.util.DaoUtil.constructCriteria;
import static org.folio.dao.util.DaoUtil.getCQL;

public class RecordDaoImpl implements RecordDao {

  private static final Logger LOG = LoggerFactory.getLogger(RecordDaoImpl.class);

  private static final String RECORDS_VIEW = "records_view";
  private static final String RESULTS_VIEW = "results_view";
  private static final String RECORDS_TABLE = "records";
  private static final String SOURCE_RECORDS_TABLE = "source_records";
  private static final String ERROR_RECORDS_TABLE = "error_records";
  private static final String ID_FIELD = "'id'";

  private PostgresClient pgClient;
  private String schema;

  public RecordDaoImpl(Vertx vertx, String tenantId) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
    this.schema = PostgresClient.convertToPsqlStandard(tenantId);
  }

  @Override
  public Future<List<Record>> getRecords(String query, int offset, int limit) {
    Future<Results<Record>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQL(RECORDS_VIEW, query, limit, offset);
      pgClient.get(RECORDS_VIEW, Record.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying records_view", e);
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id) {
    Future<Results<Record>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClient.get(RECORDS_VIEW, Record.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying records_view by id", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(records -> records.isEmpty() ? Optional.empty() : Optional.of(records.get(0)));
  }

  @Override
  public Future<Boolean> saveRecord(Record record) {
    Future<UpdateResult> future = Future.future();
    try {
      String insertQuery = constructInsertOrUpdateQuery(record);
      pgClient.execute(insertQuery, future.completer());
    } catch (Exception e) {
      LOG.error("Error while inserting new record", e);
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<Boolean> updateRecord(Record record) {
    Future<UpdateResult> future = Future.future();
    try {
      String updateQuery = constructInsertOrUpdateQuery(record);
      pgClient.execute(updateQuery, future.completer());
    } catch (Exception e) {
      LOG.error("Error while updating a record", e);
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  /**
   * currently the method is implemented allowing to delete the Record,
   * this behavior will be changed
   * (@link https://issues.folio.org/browse/MODSOURCE-16)
   */
  @Override
  public Future<Boolean> deleteRecord(String id) {
    Future<UpdateResult> future = Future.future();
    return getRecordById(id)
      .compose(optionalRecord -> optionalRecord
        .map(record -> {
          try {
            String deleteQuery = constructDeleteQuery(record);
            pgClient.execute(deleteQuery, future.completer());
          } catch (Exception e) {
            LOG.error("Error while deleting a record", e);
            future.fail(e);
          }
          return future.map(updateResult -> updateResult.getUpdated() == 1);
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Record with id '%s' was not found", id))))
      );
  }

  @Override
  public Future<List<Result>> getResults(String query, int offset, int limit) {
    Future<Results<Result>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQL(RESULTS_VIEW, query, limit, offset);
      pgClient.get(RESULTS_VIEW, Result.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying results_view", e);
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  private String constructInsertOrUpdateQuery(Record record) {
    List<String> statements = new ArrayList<>();
    RecordModel recordModel = new RecordModel()
      .withId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withMatchedProfileId(record.getMatchedProfileId())
      .withMatchedId(record.getMatchedId())
      .withGeneration(record.getGeneration())
      .withRecordType(RecordModel.RecordType.fromValue(record.getRecordType().value()))
      .withSourceRecordId(record.getSourceRecord().getId());
    SourceRecord sourceRecord = record.getSourceRecord();
    statements.add(
      constructInsertOrUpdateStatement(SOURCE_RECORDS_TABLE, sourceRecord.getId(), JsonObject.mapFrom(sourceRecord)));
    ParsedRecord parsedRecord = record.getParsedRecord();
    if (parsedRecord != null) {
      recordModel.setParsedRecordId(parsedRecord.getId());
      statements.add(constructInsertOrUpdateStatement(RecordType.valueOf(record.getRecordType().value()).getTableName(),
              parsedRecord.getId(), JsonObject.mapFrom(parsedRecord)));
    }
    ErrorRecord errorRecord = record.getErrorRecord();
    if (errorRecord != null) {
      recordModel.setErrorRecordId(errorRecord.getId());
      statements.add(
        constructInsertOrUpdateStatement(ERROR_RECORDS_TABLE, errorRecord.getId(), JsonObject.mapFrom(errorRecord)));
    }
    statements.add(
      constructInsertOrUpdateStatement(RECORDS_TABLE, recordModel.getId(), JsonObject.mapFrom(recordModel))
    );
    return String.join("", statements);
  }

  private String constructDeleteQuery(Record record) {
    List<String> statements = new ArrayList<>();
    statements.add(constructDeleteStatement(SOURCE_RECORDS_TABLE, record.getSourceRecord().getId()));
    if (record.getParsedRecord() != null) {
      statements.add(constructDeleteStatement(RecordType.valueOf(record.getRecordType().value()).getTableName(),
        record.getParsedRecord().getId()));
    }
    if (record.getErrorRecord() != null) {
      statements.add(
        constructDeleteStatement(ERROR_RECORDS_TABLE, record.getErrorRecord().getId()));
    }
    statements.add(
      constructDeleteStatement(RECORDS_TABLE, record.getId()));
    return String.join("", statements);
  }

  private String constructInsertOrUpdateStatement(String tableName, String id, JsonObject jsonData) {
    return new StringBuilder()
      .append("INSERT INTO ")
      .append(schema).append(".").append(tableName)
      .append("(_id, jsonb) VALUES ('")
      .append(id).append("', '")
      .append(jsonData).append("')")
      .append(" ON CONFLICT (_id) DO UPDATE SET jsonb = '")
      .append(jsonData).append("';").toString();
  }

  private String constructDeleteStatement(String tableName, String id) {
    return new StringBuilder()
      .append("DELETE FROM ")
      .append(schema).append(".").append(tableName)
      .append(" WHERE _id = '").append(id).append("';").toString();
  }

}
