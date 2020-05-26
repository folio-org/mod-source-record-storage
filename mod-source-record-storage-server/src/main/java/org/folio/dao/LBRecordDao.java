package org.folio.dao;

import java.util.Optional;

import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
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
  Future<Optional<Record>> getByMatchedId(String matchedId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param connection connection
   * @param matchedId  record matched id
   * @param tenantId   tenant id
   * @return future with optional record
   */
  Future<Optional<Record>> getByMatchedId(SqlConnection connection, String matchedId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param instanceId external ids holder instance id of record
   * @param tenantId   tenant id
   * @return future with optional record
   */
  Future<Optional<Record>> getByInstanceId(String instanceId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param connection connection
   * @param instanceId external ids holder instance id of record
   * @param tenantId   tenant id
   * @return future with optional record
   */
  Future<Optional<Record>> getByInstanceId(SqlConnection connection, String instanceId, String tenantId);

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
  Future<Optional<Record>> getRecordById(String id, IncomingIdType idType, String tenantId);

  /**
   * Searches for source record by id via specific {@link IncomingIdType}
   *
   * @param connection connection
   * @param id         for searching
   * @param idType     search type
   * @param tenantId   tenant id
   * @return future with optional record
   */
  Future<Optional<Record>> getRecordById(SqlConnection connection, String id, IncomingIdType idType, String tenantId);

  /**
   * Searches for {@link Record} by id of external entity which was created from desired record
   *
   * @param externalId     external relation id
   * @param externalIdType external id type
   * @param tenantId       tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByExternalId(String externalId, ExternalIdType externalIdType, String tenantId);

  /**
   * Searches for {@link Record} by id of external entity which was created from desired record
   *
   * @param connection     connection
   * @param externalId     external relation id
   * @param externalIdType external id type
   * @param tenantId       tenant id
   * @return future with optional {@link Record}
   */
  Future<Optional<Record>> getRecordByExternalId(SqlConnection connection, String externalId, ExternalIdType externalIdType, String tenantId);

}