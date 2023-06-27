package org.folio.services.migrations;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.AsyncMigrationJobDaoImpl;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.folio.services.AbstractLBServiceTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

import static org.folio.rest.jaxrs.model.AsyncMigrationJob.Status.ERROR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class AsyncMigrationJobServiceImplTest extends AbstractLBServiceTest {

  private AsyncMigrationJobService asyncMigrationJobService;
  private AsyncMigrationTaskRunner asyncMigrationTaskRunnerMock;

  @Before
  public void setUp() {
    asyncMigrationTaskRunnerMock = Mockito.mock(AsyncMigrationTaskRunner.class);
    asyncMigrationJobService = new AsyncMigrationJobServiceImpl(new AsyncMigrationJobDaoImpl(postgresClientFactory), List.of(asyncMigrationTaskRunnerMock));
  }

  @Test
  public void shouldSetAsyncMigrationJobStatusToErrorIfErrorOccursDuringMigrationExecution(TestContext context) {
    Async async = context.async();
    String migrationName = "Test-migration";
    AsyncMigrationJobInitRq migrationJobInitRequest = new AsyncMigrationJobInitRq().withMigrations(List.of(migrationName));
    when(asyncMigrationTaskRunnerMock.getMigrationName()).thenReturn(migrationName);
    when(asyncMigrationTaskRunnerMock.runMigration(any(AsyncMigrationJob.class), eq(TENANT_ID)))
      .thenReturn(Future.failedFuture("migration error"));

    Future<Optional<AsyncMigrationJob>> future = asyncMigrationJobService.runAsyncMigration(migrationJobInitRequest, TENANT_ID)
      .compose(migrationJob -> asyncMigrationJobService.getById(migrationJob.getId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<AsyncMigrationJob> migrationJobOptional = ar.result();
      context.assertTrue(migrationJobOptional.isPresent());
      context.assertEquals(migrationJobOptional.get().getStatus(), ERROR);
      async.complete();
    });
  }

}
