package org.folio.services.migrations;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.impl.TenantAPI;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@Component
public class MarcIndexersVersionMigrationTaskRunner implements AsyncMigrationTaskRunner {

  private static final Logger LOG = LogManager.getLogger();
  private static final String MIGRATION_NAME = "marcIndexersVersionMigration";
  private static final String DELETE_OLD_INDEXERS_SCRIPT_PATH = "async_migration/delete-old-records-marc-indexers.sql";
  private static final String SET_MARC_INDEXERS_VERSION_SCRIPT_PATH = "async_migration/set-marc-indexers-version.sql";

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public MarcIndexersVersionMigrationTaskRunner(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Void> runMigration(AsyncMigrationJob asyncMigrationJob, String tenantId) {
    return executeScript(DELETE_OLD_INDEXERS_SCRIPT_PATH, tenantId, asyncMigrationJob)
      .compose(v -> executeScript(SET_MARC_INDEXERS_VERSION_SCRIPT_PATH, tenantId, asyncMigrationJob))
      .onSuccess(v -> LOG.info("runMigration:: Migration '{}' successfully executed, migration jobId: '{}'", MIGRATION_NAME, asyncMigrationJob.getId()))
      .onFailure(e -> LOG.error("runMigration:: Failed to execute migration '{}', migration jobId: '{}', cause: ",
        MIGRATION_NAME, asyncMigrationJob.getId(), e));
  }

  private Future<Void> executeScript(String scriptPath, String tenantId, AsyncMigrationJob asyncMigrationJob) {
    try {
      LOG.trace("executeScript:: Trying to execute script: '{}', migration jobId: '{}'", scriptPath, asyncMigrationJob.getId());
      InputStream scriptInputStream = TenantAPI.class.getClassLoader().getResourceAsStream(scriptPath);
      String script = IOUtils.toString(scriptInputStream, StandardCharsets.UTF_8);
      Promise<Void> promise = Promise.promise();

      postgresClientFactory.getCachedPool(tenantId).query(script).execute(rowsAr -> {
        if (rowsAr.succeeded()) {
          LOG.info("executeScript:: The script: '{}' successfully executed, migration jobId: '{}'", scriptPath, asyncMigrationJob.getId());
          promise.complete();
        } else {
          LOG.error("executeScript:: Failed to execute script: '{}', migration jobId: '{}', cause: ",
            scriptPath, asyncMigrationJob.getId(), rowsAr.cause());
          promise.fail(rowsAr.cause());
        }
      });
      return promise.future();
    } catch (Exception e) {
      LOG.error("executeScript:: Failed to run script '{}', migration jobId: '{}', cause: ", scriptPath, asyncMigrationJob.getId(), e);
      return Future.failedFuture(e);
    }
  }

  @Override
  public String getMigrationName() {
    return MIGRATION_NAME;
  }
}
