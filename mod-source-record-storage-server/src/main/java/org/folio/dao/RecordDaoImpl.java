package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    Future<UpdateResult> future = Future.future();
    try {
      String insertQuery = constructInsertOrUpdateQuery(record, tenantId);
      pgClientFactory.createInstance(tenantId).execute(insertQuery, future.completer());
    } catch (Exception e) {
      LOG.error("Error while inserting new record", e);
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<Boolean> updateRecord(Record record, String tenantId) {
    Future<UpdateResult> future = Future.future();
    try {
      String updateQuery = constructInsertOrUpdateQuery(record, tenantId);
      pgClientFactory.createInstance(tenantId).execute(updateQuery, future.completer());
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
  public Future<Boolean> deleteRecord(String id, String tenantId) {
    Future<UpdateResult> future = Future.future();
    return getRecordById(id, tenantId)
      .compose(optionalRecord -> optionalRecord
        .map(record -> {
          try {
            String deleteQuery = constructDeleteQuery(record, tenantId);
            pgClientFactory.createInstance(tenantId).execute(deleteQuery, future.completer());
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
  public Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, String tenantId) {
    Future<Results<SourceRecord>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(SOURCE_RECORDS_VIEW, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(SOURCE_RECORDS_VIEW, SourceRecord.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying results_view", e);
      future.fail(e);
    }
    return future.map(results -> new SourceRecordCollection()
      .withSourceRecords(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  private String constructInsertOrUpdateQuery(Record record, String tenantId) throws Exception {
    List<String> statements = new ArrayList<>();
    RecordModel recordModel = new RecordModel()
      .withId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withMatchedProfileId(record.getMatchedProfileId())
      .withMatchedId(record.getMatchedId())
      .withGeneration(record.getGeneration())
      .withRecordType(RecordModel.RecordType.fromValue(record.getRecordType().value()))
      .withRawRecordId(record.getRawRecord().getId())
      .withMetadata(record.getMetadata());
    RawRecord rawRecord = record.getRawRecord();
    statements.add(
      constructInsertOrUpdateStatement(RAW_RECORDS_TABLE, rawRecord.getId(), pojo2json(rawRecord), tenantId));
    ParsedRecord parsedRecord = record.getParsedRecord();
    if (parsedRecord != null) {
      recordModel.setParsedRecordId(parsedRecord.getId());
      statements.add(constructInsertOrUpdateStatement(RecordType.valueOf(record.getRecordType().value()).getTableName(),
              parsedRecord.getId(), pojo2json(parsedRecord), tenantId));
    }
    ErrorRecord errorRecord = record.getErrorRecord();
    if (errorRecord != null) {
      recordModel.setErrorRecordId(errorRecord.getId());
      statements.add(
        constructInsertOrUpdateStatement(ERROR_RECORDS_TABLE, errorRecord.getId(), pojo2json(errorRecord), tenantId));
    }
    statements.add(
      constructInsertOrUpdateStatement(RECORDS_TABLE, recordModel.getId(), pojo2json(recordModel), tenantId)
    );
    return String.join("", statements);
  }

  private String constructDeleteQuery(Record record, String tenantId) {
    List<String> statements = new ArrayList<>();
    statements.add(constructDeleteStatement(RAW_RECORDS_TABLE, record.getRawRecord().getId(), tenantId));
    if (record.getParsedRecord() != null) {
      statements.add(constructDeleteStatement(RecordType.valueOf(record.getRecordType().value()).getTableName(),
        record.getParsedRecord().getId(), tenantId));
    }
    if (record.getErrorRecord() != null) {
      statements.add(
        constructDeleteStatement(ERROR_RECORDS_TABLE, record.getErrorRecord().getId(), tenantId));
    }
    statements.add(
      constructDeleteStatement(RECORDS_TABLE, record.getId(), tenantId));
    return String.join("", statements);
  }

  private String constructInsertOrUpdateStatement(String tableName, String id, String jsonData, String tenantId) {
    return new StringBuilder()
      .append("INSERT INTO ")
      .append(PostgresClient.convertToPsqlStandard(tenantId)).append(".").append(tableName)
      .append("(_id, jsonb) VALUES ('")
      .append(id).append("', '")
      .append(jsonData).append("')")
      .append(" ON CONFLICT (_id) DO UPDATE SET jsonb = '")
      .append(jsonData).append("';").toString();
  }

  private String constructDeleteStatement(String tableName, String id, String tenantId) {
    return new StringBuilder()
      .append("DELETE FROM ")
      .append(PostgresClient.convertToPsqlStandard(tenantId)).append(".").append(tableName)
      .append(" WHERE _id = '").append(id).append("';").toString();
  }

}
