package org.folio.dao;

import java.util.Optional;

import org.folio.dao.filter.RecordFilter;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;

import io.vertx.core.Future;

/**
 * Data access object for {@link Record}
 */
public interface LBRecordDao extends BeanDao<Record, RecordCollection, RecordFilter> {

  /**
   * Searches for {@link Record} by id
   * 
   * @param id       {@link Record} matched id
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  public Future<Optional<Record>> getByMatchedId(String matchedId, String tenantId);

  /**
   * Searches for {@link Record} by id
   * 
   * @param id       {@link ExternalIdsHolder} instance id of {@link Record}
   * @param tenantId tenant id
   * @return future with optional {@link Record}
   */
  public Future<Optional<Record>> getByInstanceId(String instanceId, String tenantId);

}