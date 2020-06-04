package org.folio.services;

import java.util.Collection;

import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;

import io.vertx.core.Future;

public interface LbSnapshotService extends SnapshotService {

  /**
   * @deprecated
   */
  @Override
  @Deprecated
  default Future<SnapshotCollection> getSnapshots(String query, int offset, int limit, String tenantId) {
    throw new UnsupportedOperationException("Lookup snapshots by CQL is no longer supported");
  }

  Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId);

}