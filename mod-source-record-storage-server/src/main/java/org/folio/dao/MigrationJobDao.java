package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;

import java.util.Optional;

public interface MigrationJobDao {

  Future<String> save(AsyncMigrationJob migrationJob, String tenantId);

  Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId);

  Future<AsyncMigrationJob> update(AsyncMigrationJob migrationJob, String tenantId);

  Future<Optional<AsyncMigrationJob>> getJobInProgress(String tenantId);
}
