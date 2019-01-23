package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;

import java.util.Optional;

/**
 * Snapshot Service
 */
public interface SnapshotService {

  /**
   * Searches for {@link Snapshot}
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @return future with {@link SnapshotCollection}
   */
  Future<SnapshotCollection> getSnapshots(String query, int offset, int limit);

  /**
   * Searches for {@link Snapshot} by id
   *
   * @param id Snapshot id
   * @return future with optional {@link Snapshot}
   */
  Future<Optional<Snapshot>> getSnapshotById(String id);

  /**
   * Saves {@link Snapshot}
   *
   * @param snapshot Snapshot to save
   * @return future
   */
  Future<String> saveSnapshot(Snapshot snapshot);

  /**
   * Updates {@link Snapshot} with given id
   *
   * @param snapshot Snapshot to update
   * @return future with true if succeeded
   */
  Future<Boolean> updateSnapshot(Snapshot snapshot);

  /**
   * Deletes {@link Snapshot} by id
   *
   * @param id Snapshot id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteSnapshot(String id);
}
