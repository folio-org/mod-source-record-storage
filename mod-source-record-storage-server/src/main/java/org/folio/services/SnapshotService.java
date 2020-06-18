package org.folio.services;

import java.util.Collection;
import java.util.Optional;

import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.vertx.core.Future;

public interface SnapshotService {

  /**
   * Searches for {@link Snapshot} by {@link Condition} and ordered by collection of {@link OrderField}
   * with offset and limit
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
   * Saves {@link Snapshot}
   *
   * @param snapshot Snapshot to save
   * @param tenantId tenant id
   * @return future with saved Snapshot
   */
  Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId);

  /**
   * Updates {@link Snapshot} with given id
   *
   * @param snapshot Snapshot to update
   * @param tenantId tenant id
   * @return future with updated Snapshot
   */
  Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId);

  /**
   * Deletes {@link Snapshot} by id
   *
   * @param id       Snapshot id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteSnapshot(String id, String tenantId);

}