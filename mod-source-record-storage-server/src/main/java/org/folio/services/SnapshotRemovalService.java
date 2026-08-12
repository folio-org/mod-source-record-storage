package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.ConnectionParams;

public interface SnapshotRemovalService {

  /**
   * Deletes snapshot and records by snapshotId. Also, deletes inventory instances associated to snapshot records
   *
   * @param snapshotId  snapshot id
   * @param params      okapi connection parameters
   * @return future with true if snapshot was deleted
   */
  Future<Boolean> deleteSnapshot(String snapshotId, ConnectionParams params);
}
