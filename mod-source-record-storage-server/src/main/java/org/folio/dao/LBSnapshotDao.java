package org.folio.dao;

import java.util.Collection;

import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.vertx.core.Future;

/**
 * Data access object for {@link Snapshot}
 */
public interface LBSnapshotDao extends SnapshotDao {

  /**
   * {@inheritDoc}
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<SnapshotCollection> getSnapshots(String query, int offset, int limit, String tenantId) {
    throw new UnsupportedOperationException("Lookup snapshots by CQL is no longer supported");
  }

  /**
   * Searches for {@link Snapshot} by {@link Condition} and ordered by collection of {@link OrderField} with offset and limit
   * 
   * @param condition   condition
   * @param orderFields fields to order by
   * @param offset      offset
   * @param limit       limit
   * @param tenantId    tenant id
   * @return future with {@link SnapshotCollection}
   */
  Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId);

}