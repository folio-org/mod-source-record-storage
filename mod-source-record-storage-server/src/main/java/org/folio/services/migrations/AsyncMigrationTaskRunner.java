package org.folio.services.migrations;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;

public interface AsyncMigrationTaskRunner {

  Future<Void> runMigration(AsyncMigrationJob asyncMigrationJob, String tenantId);

  String getMigrationName();

}
