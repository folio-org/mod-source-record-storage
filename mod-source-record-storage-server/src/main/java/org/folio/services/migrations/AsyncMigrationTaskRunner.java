package org.folio.services.migrations;

import io.vertx.core.Future;

public interface AsyncMigrationTaskRunner {

  Future<Void> runMigration(String tenantId);

  String getMigrationName();

}
