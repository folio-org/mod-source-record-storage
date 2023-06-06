package org.folio.services.migrations;

import io.vertx.core.Future;
import org.folio.dao.MigrationJobDao;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

@Service
public class AsyncMigrationJobServiceImpl implements AsyncMigrationJobService {

  private MigrationJobDao migrationJobDao;

  @Autowired
  public AsyncMigrationJobServiceImpl(MigrationJobDao migrationJobDao) {
    this.migrationJobDao = migrationJobDao;
  }

  @Override
  public Future<Void> runAsyncMigration(AsyncMigrationJobInitRq migrationJobInitRq, String tenantId) {
    AsyncMigrationJob asyncMigrationJob = new AsyncMigrationJob()
      .withId(UUID.randomUUID().toString())
      .withMigrations(migrationJobInitRq.getMigrations())
      .withStatus(AsyncMigrationJob.Status.IN_PROGRESS)
      .withStartedDate(new Date());

    return migrationJobDao.save(asyncMigrationJob, tenantId)
      .mapEmpty();
  }

  @Override
  public Future<Optional<AsyncMigrationJob>> getById(String id, String tenantId) {
    return migrationJobDao.getById(id, tenantId);
  }

}
