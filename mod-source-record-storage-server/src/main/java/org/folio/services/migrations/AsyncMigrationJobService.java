package org.folio.services.migrations;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;

import java.util.Optional;

/**
 * AsyncMigrationJob service interface
 */
public interface AsyncMigrationJobService {

  /**
   * Executes asynchronous migration according to specified {@code migrations} in {@link AsyncMigrationJobInitRq}.
   * Returns future with {@code AsyncMigrationJob} that reflects state of asynchronous migration execution.
   * The method completes returned future before asynchronous migration execution is finished.
   *
   * @param migrationJobInitRq  request DTO to initiate asynchronous migration job execution
   * @param tenantId            tenant id
   * @return future of {@link AsyncMigrationJob} that reflects state of asynchronous migration execution
   */
  Future<AsyncMigrationJob> runAsyncMigration(AsyncMigrationJobInitRq migrationJobInitRq, String tenantId);

  /**
   * Searches for {@link AsyncMigrationJob} by id
   *
   * @param id        asyncMigrationJob migration job id
   * @param tenantId  tenant id
   * @return future with Optional of {@link AsyncMigrationJob}
   */
  Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId);

}
