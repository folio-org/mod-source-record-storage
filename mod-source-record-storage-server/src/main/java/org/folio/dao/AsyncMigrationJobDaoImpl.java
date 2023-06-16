package org.folio.dao;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jooq.enums.MigrationJobStatus;
import org.folio.rest.jooq.tables.mappers.RowMappers;
import org.folio.rest.jooq.tables.pojos.AsyncMigrationJobs;
import org.folio.rest.jooq.tables.records.AsyncMigrationJobsRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.NotFoundException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.jooq.Tables.ASYNC_MIGRATION_JOBS;

@Repository
public class AsyncMigrationJobDaoImpl implements AsyncMigrationJobDao {

  private static final Logger LOG = LogManager.getLogger();
  private static final String JOB_NOT_FOUND_MSG = "Async migration job was not found by id: '%s'";

  private PostgresClientFactory postgresClientFactory;

  @Autowired
  public AsyncMigrationJobDaoImpl(PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<String> save(AsyncMigrationJob migrationJob, String tenantId) {
    LOG.trace("save:: Saving async migration job with id {} for tenant {}", migrationJob.getId(), tenantId);
    return getQueryExecutor(tenantId).executeAny(dslContext -> dslContext
        .insertInto(ASYNC_MIGRATION_JOBS)
        .set(mapToDatabaseRecord(migrationJob)))
      .map(migrationJob.getId());
  }

  @Override
  public Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId) {
    LOG.trace("getById:: Searching async migration job by id {} for tenant {}", id, tenantId);
    return getQueryExecutor(tenantId).findOneRow(dslContext -> dslContext
        .selectFrom(ASYNC_MIGRATION_JOBS)
        .where(ASYNC_MIGRATION_JOBS.ID.eq(UUID.fromString(id))))
      .map(row -> row != null ? Optional.of(mapRowToAsyncMigrationJob(row)) : Optional.empty());
  }

  @Override
  public Future<AsyncMigrationJob> update(AsyncMigrationJob migrationJob, String tenantId) {
    LOG.trace("update:: Updating async migration job by id {} for tenant {}", migrationJob.getId(), tenantId);

    return getQueryExecutor(tenantId).executeAny(dslContext -> dslContext
      .update(ASYNC_MIGRATION_JOBS)
      .set(mapToDatabaseRecord(migrationJob))
      .where(ASYNC_MIGRATION_JOBS.ID.eq(UUID.fromString(migrationJob.getId())))
      .returning())
      .compose(rows -> rows.size() != 0 ? Future.succeededFuture(migrationJob)
        : Future.failedFuture(new NotFoundException(format(JOB_NOT_FOUND_MSG, migrationJob.getId()))));
  }

  @Override
  public Future<Optional<AsyncMigrationJob>> getJobInProgress(String tenantId) {
    LOG.trace("getJobInProgress:: Searching async migration job  status for tenant {}", tenantId);
    return getQueryExecutor(tenantId).findOneRow(dslContext -> dslContext
        .selectFrom(ASYNC_MIGRATION_JOBS)
        .where(ASYNC_MIGRATION_JOBS.STATUS.eq(MigrationJobStatus.IN_PROGRESS)))
      .map(row -> row != null ? Optional.of(mapRowToAsyncMigrationJob(row)) : Optional.empty());
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private AsyncMigrationJobsRecord mapToDatabaseRecord(AsyncMigrationJob migrationJob) {
    AsyncMigrationJobsRecord asyncMigrationJobsDbRecord = new AsyncMigrationJobsRecord();
    asyncMigrationJobsDbRecord.setMigrations(migrationJob.getMigrations().toArray(new String[0]));

    if (migrationJob.getId() != null) {
      asyncMigrationJobsDbRecord.setId(UUID.fromString(migrationJob.getId()));
    }
    if (migrationJob.getStatus() != null) {
      asyncMigrationJobsDbRecord.setStatus(MigrationJobStatus.valueOf(migrationJob.getStatus().toString()));
    }
    if (migrationJob.getStartedDate() != null) {
      asyncMigrationJobsDbRecord.setStartedDate(migrationJob.getStartedDate().toInstant().atOffset(ZoneOffset.UTC));
    }
    if (migrationJob.getCompletedDate() != null) {
      asyncMigrationJobsDbRecord.setCompletedDate(migrationJob.getCompletedDate().toInstant().atOffset(ZoneOffset.UTC));
    }
    if (migrationJob.getErrorMessage() != null) {
      asyncMigrationJobsDbRecord.setError(migrationJob.getErrorMessage());
    }
    return asyncMigrationJobsDbRecord;
  }

  private AsyncMigrationJob mapRowToAsyncMigrationJob(Row row) {
    AsyncMigrationJobs pojo = RowMappers.getAsyncMigrationJobsMapper().apply(row);
    AsyncMigrationJob asyncMigrationJob = new AsyncMigrationJob()
      .withId(pojo.getId().toString())
      .withMigrations(Arrays.asList(row.getArrayOfStrings(ASYNC_MIGRATION_JOBS.MIGRATIONS.getName())))
      .withStatus(AsyncMigrationJob.Status.fromValue(pojo.getStatus().toString()))
      .withErrorMessage(pojo.getError());

    if (pojo.getStartedDate() != null) {
      asyncMigrationJob.withStartedDate(Date.from(pojo.getStartedDate().toInstant()));
    }
    if (pojo.getCompletedDate() != null) {
      asyncMigrationJob.withCompletedDate(Date.from(pojo.getCompletedDate().toInstant()));
    }
    return asyncMigrationJob;
  }

}
