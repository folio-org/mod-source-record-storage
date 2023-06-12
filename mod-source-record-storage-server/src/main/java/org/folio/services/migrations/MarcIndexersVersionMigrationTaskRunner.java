package org.folio.services.migrations;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.impl.TenantAPI;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Component
public class MarcIndexersVersionMigrationTaskRunner implements AsyncMigrationTaskRunner {

  private static final Logger LOG = LogManager.getLogger();
  private static final String MIGRATION_NAME = "marcIndexersVersionMigration";
  private static final String SCRIPT_PATH = "async_migration/update-fill-in-trigger.sql";

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public MarcIndexersVersionMigrationTaskRunner(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Void> runMigration(String tenantId) {
    try {
      InputStream scriptInputStream = TenantAPI.class.getClassLoader().getResourceAsStream(SCRIPT_PATH);
      String script = IOUtils.toString(scriptInputStream, StandardCharsets.UTF_8);
      Promise<Void> promise = Promise.promise();

      postgresClientFactory.getCachedPool(tenantId).query(script).execute(rowsAr -> {
        if (rowsAr.succeeded()) {
          LOG.info("runMigration:: Migration '{}' successfully executed", MIGRATION_NAME);
          promise.complete();
        } else {
          LOG.error("runMigration:: Failed to execute migration '{}', cause: ", MIGRATION_NAME, rowsAr.cause());
          promise.fail(rowsAr.cause());
        }
      });
      return promise.future();
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  @Override
  public String getMigrationName() {
    return MIGRATION_NAME;
  }
}
