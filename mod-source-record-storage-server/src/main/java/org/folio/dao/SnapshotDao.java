package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;

import java.util.Optional;

/**
 * Data access object for {@link Snapshot}
 */
public interface SnapshotDao {

  String SNAPSHOTS_TABLE = "snapshots";
  String SNAPSHOT_ID_FIELD = "'jobExecutionId'";

  /**
   * Searches for {@link Snapshot} in database
   *
   * @param query    query string to filter snapshots based on matching criteria in fields
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with {@link SnapshotCollection}
   */
  Future<SnapshotCollection> getSnapshots(String query, int offset, int limit, String tenantId);

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
