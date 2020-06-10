package org.folio.dao;

import java.util.Collection;
import java.util.Optional;

import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.vertx.core.Future;

/**
 * Data access object for {@link Snapshot}
 */
public interface SnapshotDao {

  /**
   * Searches for {@link Snapshot} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   * 
   * @param condition   query where condition
   * @param orderFields fields to order by
   * @param offset      starting index in a list of results
   * @param limit       limit of records for pagination
   * @param tenantId    tenant id
   * @return future with {@link SnapshotCollection}
   */
  Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId);

  /**
   * Searches for {@link Snapshot} by id
   *
   * @param id       Snapshot id
   * @param tenantId tenant id
   * @return future with optional {@link Snapshot}
   */
  Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId);

  /**
   * Saves {@link Snapshot} to database
   *
   * @param snapshot {@link Snapshot} to save
   * @param tenantId tenant id
   * @return future with saved entity
   */
  Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId);

  /**
   * Updates {@link Snapshot} in database
   *
   * @param snapshot {@link Snapshot} to update
   * @param tenantId tenant id
   * @return future with updated entity
   */
  Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId);

  /**
   * Deletes {@link Snapshot} from database
   *
   * @param id       id of {@link Snapshot} to delete
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteSnapshot(String id, String tenantId);

}