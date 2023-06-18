package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;

import java.util.Optional;

/**
 * Data access object for {@link AsyncMigrationJob} entity
 *
 * @see AsyncMigrationJob
 */
public interface AsyncMigrationJobDao {

  /**
   * Saves {@link AsyncMigrationJob}
   *
   * @param migrationJob asynchronous migration job to save
   * @param tenantId     tenant id
   * @return future of Void
   */
  Future<Void> save(AsyncMigrationJob migrationJob, String tenantId);

  /**
   * Searches for {@link AsyncMigrationJob} by id
   *
   * @param id        asyncMigrationJob migration job id
   * @param tenantId  tenant id
   * @return future with Optional of {@link AsyncMigrationJob}
   */
  Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId);

  /**
   * Updates {@link AsyncMigrationJob}
   *
   * @param migrationJob  asyncMigrationJob to update
   * @param tenantId      tenant id
   * @return future with updated asyncMigrationJob
   */
  Future<AsyncMigrationJob> update(AsyncMigrationJob migrationJob, String tenantId);

  /**
   * Searches for {@link AsyncMigrationJob} with status IN_PROGRESS
   *
   * @param tenantId tenant id
   * @return future with Optional of {@link AsyncMigrationJob}
   */
  Future<Optional<AsyncMigrationJob>> getJobInProgress(String tenantId);
}
