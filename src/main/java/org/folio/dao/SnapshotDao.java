package org.folio.dao;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Snapshot;

/**
 * Data access object for {@link Snapshot}
 */
public interface SnapshotDao {

  /**
   * Searches for {@link Snapshot} in database
   *
   * @param query  query string to filter snapshots based on matching criteria in fields
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with list of {@link Snapshot}
   */
  Future<List<Snapshot>> getSnapshots(String query, int offset, int limit);

  /**
   * Searches for {@link Snapshot} by id
   *
   * @param id Snapshot id
   * @return future with optional {@link Snapshot}
   */
  Future<Optional<Snapshot>> getSnapshotById(String id);

  /**
   * Saves {@link Snapshot} to database
   *
   * @param snapshot {@link Snapshot} to save
   * @return future
   */
  Future<String> saveSnapshot(Snapshot snapshot);

  /**
   * Updates {@link Snapshot} in database
   *
   * @param snapshot {@link Snapshot} to update
   * @return future with true if succeeded
   */
  Future<Boolean> updateSnapshot(Snapshot snapshot);

  /**
   * Deletes {@link Snapshot} from database
   *
   * @param id id of {@link Snapshot} to delete
   * @return future with true if succeeded
   */
  Future<Boolean> deleteSnapshot(String id);
}
