package org.folio.services.migrations;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;

import java.util.Optional;

public interface AsyncMigrationJobService {

  Future<AsyncMigrationJob> runAsyncMigration(AsyncMigrationJobInitRq migrationJobInitRq, String tenantId);

  Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId);

}
