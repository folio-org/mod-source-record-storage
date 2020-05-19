package org.folio.dao;

import java.util.Optional;

import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;

import io.vertx.core.Future;
import io.vertx.sqlclient.SqlConnection;

/**
 * Data access object for {@link Record}
 */
public interface LBRecordDao extends EntityDao<Record, RecordCollection, RecordQuery> {

  /**
   * Searches for {@link Record} by id
   * 
   * @param matchedId record matched id
   * @param tenantId  tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getByMatchedId(String matchedId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param connection connection
   * @param matchedId  record matched id
   * @param tenantId   tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getByMatchedId(SqlConnection connection, String matchedId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param instanceId external ids holder instance id of record
   * @param tenantId   tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getByInstanceId(String instanceId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param connection connection
   * @param instanceId external ids holder instance id of record
   * @param tenantId   tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getByInstanceId(SqlConnection connection, String instanceId, String tenantId);

  /**
   * Increments generation in case a record with the same matchedId exists
   * and the snapshot it is linked to is COMMITTED before the processing
   * of the current one started
   *
   * @param record   record
   * @param tenantId tenant id
   * @return future with generation
   */
  Future<Integer> calculateGeneration(Record record, String tenantId);

  /**
   * Increments generation in case a record with the same matchedId exists
   * and the snapshot it is linked to is COMMITTED before the processing
   * of the current one started
   *
   * @param connection connection
   * @param record     record
   * @param tenantId   tenant id
   * @return future with generation
   */
  Future<Integer> calculateGeneration(SqlConnection connection, Record record, String tenantId);

  /**
   * Searches for source record by id via specific {@link IncomingIdType}
   *
   * @param id       for searching
   * @param idType   search type
   * @param tenantId tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getRecordById(String id, IncomingIdType idType, String tenantId);

  /**
   * Searches for source record by id via specific {@link IncomingIdType}
   *
   * @param connection connection
   * @param id         for searching
   * @param idType     search type
   * @param tenantId   tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getRecordById(SqlConnection connection, String id, IncomingIdType idType, String tenantId);

  /**
   * Change suppress from discovery flag for record by external relation id
   *
   * @param suppressFromDiscoveryDto dto that contains new value and id
   * @param tenantId                 tenant id
   * @return - future with record if succeeded
   */
  public Future<Record> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId);

  /**
   * Creates new updated Record with incremented generation linked to a new Snapshot, and sets OLD status to the "old" Record,
   * no data is deleted as a result of the update
   *
   * @param parsedRecordDto parsed record DTO containing updates to parsed record
   * @param snapshotId      snapshot id to which new Record should be linked
   * @param tenantId        tenant id
   * @return future with updated Record
   */
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId);

}