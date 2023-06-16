package org.folio.services.migrations;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.AsyncMigrationJobDao;
import org.folio.dataimport.util.exception.ConflictException;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.BadRequestException;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class AsyncMigrationJobServiceImpl implements AsyncMigrationJobService {

  private static final Logger LOG = LogManager.getLogger();
  private static final String INVALID_MIGRATIONS_MSG = "Specified migrations are not supported. Migrations: %s";
  private static final String MIGRATION_IN_PROGRESS_MSG = "Failed to initiate migration job, because migration job with id '%s' already in progress";
  private static final String ERROR_UPDATE_JOB_STATUS_MSG = "Error updating migration job status to '%s', jobId: '%s'";

  private AsyncMigrationJobDao migrationJobDao;
  private List<AsyncMigrationTaskRunner> jobRunners;

  @Autowired
  public AsyncMigrationJobServiceImpl(AsyncMigrationJobDao migrationJobDao, List<AsyncMigrationTaskRunner> jobRunners) {
    this.migrationJobDao = migrationJobDao;
    this.jobRunners = jobRunners;
  }

  @Override
  public Future<AsyncMigrationJob> runAsyncMigration(AsyncMigrationJobInitRq migrationJobInitRq, String tenantId) {
    List<String> invalidMigrations = getUnsupportedMigrations(migrationJobInitRq);
    if (!invalidMigrations.isEmpty()) {
      return Future.failedFuture(new BadRequestException(String.format(INVALID_MIGRATIONS_MSG, invalidMigrations)));
    }

    AsyncMigrationJob asyncMigrationJob = new AsyncMigrationJob()
      .withId(UUID.randomUUID().toString())
      .withMigrations(migrationJobInitRq.getMigrations())
      .withStatus(AsyncMigrationJob.Status.IN_PROGRESS)
      .withStartedDate(new Date());

    return checkMigrationsInProgress(tenantId)
      .compose(v -> migrationJobDao.save(asyncMigrationJob, tenantId))
      .map(res -> {
        runMigrations(asyncMigrationJob, tenantId)
          .onSuccess(v -> logProcessedMigration(asyncMigrationJob, tenantId))
          .onFailure(e -> logFailedMigration(asyncMigrationJob, tenantId, e));
        return asyncMigrationJob;
      });
  }

  private List<String> getUnsupportedMigrations(AsyncMigrationJobInitRq migrationJobInitRq) {
    List<String> migrations = migrationJobInitRq.getMigrations();
    return migrations.stream()
      .filter(migrationName -> jobRunners.stream().noneMatch(jobRunner -> jobRunner.getMigrationName().equals(migrationName)))
      .collect(Collectors.toList());
  }

  private Future<Boolean> checkMigrationsInProgress(String tenantId) {
    return migrationJobDao.getJobInProgress(tenantId).compose(jobOptional -> {
      if (jobOptional.isPresent()) {
        String msg = String.format(MIGRATION_IN_PROGRESS_MSG, jobOptional.get().getId());
        LOG.warn("checkMigrationsInProgress:: {}", msg);
        return Future.failedFuture(new ConflictException(msg));
      }
      return Future.succeededFuture(false);
    });
  }

  private Future<Void> runMigrations(AsyncMigrationJob asyncMigrationJob, String tenantId) {
    List<AsyncMigrationTaskRunner> runners = asyncMigrationJob.getMigrations().stream()
      .flatMap(migrationName -> jobRunners.stream().filter(runner -> migrationName.equals(runner.getMigrationName())))
      .collect(Collectors.toList());

    Future<Void> future = Future.succeededFuture();
    for (AsyncMigrationTaskRunner runner : runners) {
      future = future.compose(v -> runner.runMigration(asyncMigrationJob, tenantId));
    }

    return future;
  }

  private void logProcessedMigration(AsyncMigrationJob asyncMigrationJob, String tenantId) {
    LOG.info("logProcessedMigration:: Async migration job with id: '{}' and migrations: '{}' was completed successfully",
      asyncMigrationJob.getId(), asyncMigrationJob.getMigrations());

    asyncMigrationJob.withCompletedDate(new Date())
      .withStatus(AsyncMigrationJob.Status.COMPLETED);
    migrationJobDao.update(asyncMigrationJob, tenantId)
      .onFailure(e -> LOG.error(String.format(ERROR_UPDATE_JOB_STATUS_MSG, asyncMigrationJob.getStatus(), asyncMigrationJob.getId()), e));
  }

  private void logFailedMigration(AsyncMigrationJob asyncMigrationJob, String tenantId, Throwable throwable) {
    LOG.error("logFailedMigration:: Async migration job with id: '{}' and migrations: '{}' failed",
      asyncMigrationJob.getId(), asyncMigrationJob.getMigrations(), throwable);

    asyncMigrationJob.withCompletedDate(new Date())
      .withStatus(AsyncMigrationJob.Status.ERROR)
      .withErrorMessage(throwable.getMessage());
    migrationJobDao.update(asyncMigrationJob, tenantId)
      .onFailure(e -> LOG.error(String.format(ERROR_UPDATE_JOB_STATUS_MSG, asyncMigrationJob.getStatus(), asyncMigrationJob.getId()), e));
  }

  @Override
  public Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId) {
    return migrationJobDao.getById(id, tenantId);
  }

}
