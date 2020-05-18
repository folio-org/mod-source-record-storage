package org.folio.dao;

import java.util.Optional;

import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;

import io.vertx.core.Future;

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
   * @param instanceId external ids holder instance id of record
   * @param tenantId   tenant id
   * @return future with optional record
   */
  public Future<Optional<Record>> getByInstanceId(String instanceId, String tenantId);

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

}