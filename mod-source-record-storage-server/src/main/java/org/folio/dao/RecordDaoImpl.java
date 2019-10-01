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
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.RecordType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordModel;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.dao.SnapshotDao.SNAPSHOTS_TABLE;
import static org.folio.dao.SnapshotDao.SNAPSHOT_ID_FIELD;
import static org.folio.dao.util.DbUtil.executeInTransaction;
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
  private static final String SNAPSHOT_FIELD = "'snapshotId'";
  private static final String GET_HIGHEST_GENERATION_QUERY = "select get_highest_generation('%s', '%s');";
  private static final String UPSERT_QUERY = "INSERT INTO %s.%s (_id, jsonb) VALUES (?, ?) ON CONFLICT (_id) DO UPDATE SET jsonb = ?;";
  private static final String GET_RECORD_BY_EXTERNAL_ID_QUERY = "select get_record_by_external_id('%s', '%s');";

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
  public Future<Record> saveRecord(Record record, String tenantId) {
    return insertOrUpdateRecord(record, tenantId);
  }

  @Override
  public Future<Record> updateRecord(Record record, String tenantId) {
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
      String getHighestGeneration = format(GET_HIGHEST_GENERATION_QUERY, record.getMatchedId(), record.getSnapshotId());
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
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    return executeInTransaction(pgClient, connection -> Future.succeededFuture()
      .compose(v -> updateExternalIdsForRecord(connection, record, tenantId))
      .compose(updated -> updateParsedRecord(connection, record.getParsedRecord(), record.getRecordType(), tenantId))
    );
  }

  /**
   * Updates external relations ids for record in transaction
   *
   * @param tx transaction connection
   * @param record record dto
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  private Future<Boolean> updateExternalIdsForRecord(AsyncResult<SQLConnection> tx, Record record, String tenantId) {
    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    String rollBackMessage = format("Record with id: %s was not found", record.getId());
    Future<Results<RecordModel>> resultSetFuture = Future.future();

    Criteria idCrit = constructCriteria(ID_FIELD, record.getId());
    pgClient.get(tx, RECORDS_TABLE, RecordModel.class, new Criterion(idCrit), true, true, resultSetFuture);
    return resultSetFuture
      .map(results -> {
        if (results.getResults().isEmpty()) {
          throw new NotFoundException(rollBackMessage);
        }
        return results.getResults().get(0);
      })
      .compose(recordModel -> {
        recordModel.setExternalIdsHolder(record.getExternalIdsHolder());
        return insertOrUpdate(tx, recordModel, recordModel.getId(), RECORDS_TABLE, tenantId)
          .map(updated -> {
            if (!updated) {
              throw new NotFoundException(rollBackMessage);
            }
            return true;
          });
      });
  }

  /**
   * Updates {@link ParsedRecord} in the db in scope of transaction
   *
   * @param tx transaction connection
   * @param parsedRecord {@link ParsedRecord} to update
   * @param recordType   type of ParsedRecord
   * @param tenantId     tenant id
   * @return future with updated ParsedRecord
   */
  private Future<ParsedRecord> updateParsedRecord(AsyncResult<SQLConnection> tx, ParsedRecord parsedRecord, Record.RecordType recordType, String tenantId) {
    Future<ParsedRecord> future = Future.future();
    try {
      CQLWrapper filter = getCQLWrapper(RecordType.valueOf(recordType.value()).getTableName(), "id==" + parsedRecord.getId());
      pgClientFactory.createInstance(tenantId).update(tx, RecordType.valueOf(recordType.value()).getTableName(),
        convertParsedRecordToJsonObject(parsedRecord), filter, true, updateResult -> {
          if (updateResult.failed()) {
            LOG.error("Could not update ParsedRecord with id {}", updateResult.cause(), parsedRecord.getId());
            future.fail(updateResult.cause());
          } else if (updateResult.result().getUpdated() != 1) {
            String errorMessage = format("ParsedRecord with id '%s' was not found", parsedRecord.getId());
            LOG.error(errorMessage);
            future.fail(new NotFoundException(errorMessage));
          } else {
            future.complete(parsedRecord);
          }
        });
    } catch (Exception e) {
      LOG.error("Error updating ParsedRecord with id {}", e, parsedRecord.getId());
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Optional<Record>> getRecordByExternalId(String externalId, ExternalIdType externalIdType, String tenantId) {
    Future<ResultSet> future = Future.future();
    try {
      String query = format(GET_RECORD_BY_EXTERNAL_ID_QUERY, externalId, externalIdType.getExternalIdField());
      pgClientFactory.createInstance(tenantId).select(query, future.completer());
    } catch (Exception e) {
      LOG.error("Error while searching for Record by instance id {}", e, externalId);
      future.fail(e);
    }
    return future.map(resultSet -> {
      String record = resultSet.getResults().get(0).getString(0);
      return Optional.ofNullable(record).map(it -> new JsonObject(it).mapTo(Record.class));
    });
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId) {

    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    String queryForRecordSearchByExternalId = constructQueryForRecordSearchByExternalId(suppressFromDiscoveryDto);
    String rollBackMessage = format("Record with %s id: %s was not found", suppressFromDiscoveryDto.getIncomingIdType().name(), suppressFromDiscoveryDto.getId());

    return executeInTransaction(pgClient, connection ->
      Future.succeededFuture()
        .compose(v -> {
          Future<ResultSet> resultSetFuture = Future.future();
          pgClient.select(connection, queryForRecordSearchByExternalId, resultSetFuture);
          return resultSetFuture
            .map(resultSet -> {
              if (resultSet.getResults() == null
                || resultSet.getResults().isEmpty()
                || resultSet.getResults().get(0) == null
                || resultSet.getResults().get(0).getString(0) == null) {
                throw new NotFoundException(rollBackMessage);
              }
              return new JsonObject(resultSet.getResults().get(0).getString(0)).mapTo(Record.class);
            });
        })
        .compose(record -> {
          Future<Results<RecordModel>> resultSetFuture = Future.future();
          Criteria idCrit = constructCriteria(ID_FIELD, record.getId());
          pgClient.get(connection, RECORDS_TABLE, RecordModel.class, new Criterion(idCrit), true, true, resultSetFuture);
          return resultSetFuture
            .map(results -> {
              if (results.getResults().isEmpty()) {
                throw new NotFoundException(format("Record with id %s was not found", record.getId()));
              }
              return results.getResults().get(0);
            });
        })
        .compose(recordModel -> {
          recordModel.getAdditionalInfo().setSuppressDiscovery(suppressFromDiscoveryDto.getSuppressFromDiscovery());
          return insertOrUpdate(connection, recordModel, recordModel.getId(), RECORDS_TABLE, tenantId)
            .map(updated -> {
              if (!updated) {
                throw new NotFoundException(rollBackMessage);
              }
              return true;
            });
        })
    );
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {

    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    Criterion snapshotIdCriterion = new Criterion(constructCriteria(SNAPSHOT_ID_FIELD, snapshotId));

    return executeInTransaction(pgClient, connection ->
      Future.succeededFuture()
        .compose(v -> {
          Future<Results<Snapshot>> snapshotFuture = Future.future();
          pgClient.get(connection, SNAPSHOTS_TABLE, Snapshot.class, snapshotIdCriterion, true, false, null, snapshotFuture.completer());
          return snapshotFuture
            .map(results -> {
              if (results.getResults().isEmpty()) {
                throw new NotFoundException(format("Snapshot with id %s was not found", snapshotId));
              }
              return results.getResults().get(0);
            });
        })
        .compose(snapshot -> {
          Future<Results<Record>> recordsFuture = Future.future();
          Criteria snapshotCriterion = constructCriteria(SNAPSHOT_FIELD, snapshot.getJobExecutionId());
          pgClient.get(connection, RECORDS_VIEW, Record.class, new Criterion(snapshotCriterion), true, false, null, recordsFuture.completer());
          return recordsFuture.map(Results::getResults);
        })
        .compose(records -> {
          Future<Boolean> deletedFuture = Future.succeededFuture(true);
          for (Record record : records) {
            deletedFuture = deletedFuture.compose(v -> deleteRecord(record, pgClient, connection));
          }
          return deletedFuture;
        })
        .compose(v -> {
          Future<UpdateResult> deleteSnapshot = Future.future();
          pgClient.delete(connection, SNAPSHOTS_TABLE, snapshotIdCriterion, deleteSnapshot.completer());
          return deleteSnapshot.map(deleted -> deleted.getUpdated() == 1);
        })
    );
  }

  /**
   * Deletes Record
   *
   * @param record         Record to delete
   * @param postgresClient Postgres Client
   * @param connection     connection
   * @return future with true if succeeded
   */
  private Future<Boolean> deleteRecord(Record record, PostgresClient postgresClient, AsyncResult<SQLConnection> connection) {
    Future<Boolean> future = Future.future();
    Future.succeededFuture()
      .compose(v -> deleteById(record.getRawRecord().getId(), RAW_RECORDS_TABLE, postgresClient, connection))
      .compose(v -> {
        if (record.getParsedRecord() != null) {
          return deleteById(record.getParsedRecord().getId(), RecordType.valueOf(record.getRecordType().value()).getTableName(), postgresClient, connection);
        }
        return Future.succeededFuture();
      })
      .compose(v -> {
        if (record.getErrorRecord() != null) {
          return deleteById(record.getErrorRecord().getId(), ERROR_RECORDS_TABLE, postgresClient, connection);
        }
        return Future.succeededFuture();
      })
      .compose(v -> deleteById(record.getId(), RECORDS_TABLE, postgresClient, connection))
      .setHandler(result -> {
        if (result.succeeded()) {
          future.complete(true);
        } else {
          LOG.error("Failed to delete Record with id {}", result.cause(), record.getId());
          future.fail(result.cause());
        }
      });
    return future;
  }

  /**
   * Deletes entity by id
   *
   * @param id             id of the entity to delete
   * @param tableName      table name
   * @param postgresClient Postgres Client
   * @param connection     connection
   * @return future with true if succeeded
   */
  private Future<Boolean> deleteById(String id, String tableName, PostgresClient postgresClient, AsyncResult<SQLConnection> connection) {
    Future<UpdateResult> deleteFuture = Future.future();
    Criteria idCrit = constructCriteria(ID_FIELD, id);
    postgresClient.delete(connection, tableName, new Criterion(idCrit), deleteFuture.completer());
    return deleteFuture.map(deleted -> deleted.getUpdated() == 1);
  }

  private Future<Record> insertOrUpdateRecord(Record record, String tenantId) {

    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    RecordModel recordModel = new RecordModel()
      .withId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withMatchedProfileId(record.getMatchedProfileId())
      .withMatchedId(record.getMatchedId())
      .withGeneration(record.getGeneration())
      .withRecordType(RecordModel.RecordType.fromValue(record.getRecordType().value()))
      .withRawRecordId(record.getRawRecord().getId())
      .withExternalIdsHolder(record.getExternalIdsHolder())
      .withDeleted(record.getDeleted())
      .withAdditionalInfo(record.getAdditionalInfo())
      .withMetadata(record.getMetadata());

    return executeInTransaction(pgClient, connection ->
      Future.succeededFuture()
        .compose(v -> insertOrUpdate(connection, record.getRawRecord(), record.getRawRecord().getId(), RAW_RECORDS_TABLE, tenantId))
        .compose(v -> {
          if (record.getParsedRecord() != null) {
            recordModel.setParsedRecordId(record.getParsedRecord().getId());
            return insertOrUpdateParsedRecord(connection, record, tenantId);
          }
          return Future.succeededFuture();
        })
        .compose(v -> {
          if (record.getErrorRecord() != null) {
            recordModel.setErrorRecordId(record.getErrorRecord().getId());
            return insertOrUpdate(connection, record.getErrorRecord(), record.getErrorRecord().getId(), ERROR_RECORDS_TABLE, tenantId);
          }
          return Future.succeededFuture();
        })
        .compose(v -> insertOrUpdate(connection, recordModel, recordModel.getId(), RECORDS_TABLE, tenantId).map(updated -> record))
    );
  }

  private Future<Boolean> insertOrUpdate(AsyncResult<SQLConnection> tx, Object rawRecord, String id, String tableName, String tenantId) {
    Future<UpdateResult> future = Future.future();
    try {
      JsonArray params = new JsonArray().add(id).add(pojo2json(rawRecord)).add(pojo2json(rawRecord));
      String query = format(UPSERT_QUERY, PostgresClient.convertToPsqlStandard(tenantId), tableName);
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
      String query = format(UPSERT_QUERY, PostgresClient.convertToPsqlStandard(tenantId), RecordType.valueOf(record.getRecordType().value()).getTableName());
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

  /**
   * Constructs query for record search by external id
   *
   * @param suppressFromDiscoveryDto SuppressDiscoveryDto containing incoming id type
   * @return query
   */
  private String constructQueryForRecordSearchByExternalId(SuppressFromDiscoveryDto suppressFromDiscoveryDto) {
    try {
      ExternalIdType externalIdType = ExternalIdType.valueOf(suppressFromDiscoveryDto.getIncomingIdType().value());
      return format(GET_RECORD_BY_EXTERNAL_ID_QUERY, suppressFromDiscoveryDto.getId(), externalIdType.getExternalIdField());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Selected IncomingIdType is not supported");
    }
  }

}
