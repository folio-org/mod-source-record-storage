package org.folio.services.migrations;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;

public interface AsyncMigrationTaskRunner {

  /**
   * Performs migration operations
   *
   * @param asyncMigrationJob asyncMigrationJob entity
   * @param tenantId          tenant id
   * @return future of Void
   */
  Future<Void> runMigration(AsyncMigrationJob asyncMigrationJob, String tenantId);

  /**
   * Returns migration task name
   *
   * @return migration task name
   */
  String getMigrationName();

}
