package org.folio.services.migrations;

import static org.folio.rest.jaxrs.model.AsyncMigrationJob.Status.ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import org.folio.dao.AsyncMigrationJobDaoImpl;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.folio.services.AbstractLBServiceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class AsyncMigrationJobServiceImplTest extends AbstractLBServiceTest {

  private AsyncMigrationJobService asyncMigrationJobService;
  private AsyncMigrationTaskRunner asyncMigrationTaskRunnerMock;

  @BeforeEach
  void setUp() {
    asyncMigrationTaskRunnerMock = Mockito.mock(AsyncMigrationTaskRunner.class);
    asyncMigrationJobService = new AsyncMigrationJobServiceImpl(new AsyncMigrationJobDaoImpl(postgresClientFactory), List.of(asyncMigrationTaskRunnerMock));
  }

  @Test
  void shouldSetAsyncMigrationJobStatusToErrorIfErrorOccursDuringMigrationExecution(VertxTestContext testContext) {
    String migrationName = "Test-migration";
    AsyncMigrationJobInitRq migrationJobInitRequest = new AsyncMigrationJobInitRq().withMigrations(List.of(migrationName));
    when(asyncMigrationTaskRunnerMock.getMigrationName()).thenReturn(migrationName);
    when(asyncMigrationTaskRunnerMock.runMigration(any(AsyncMigrationJob.class), eq(TENANT_ID)))
      .thenReturn(Future.failedFuture("migration error"));

    asyncMigrationJobService.runAsyncMigration(migrationJobInitRequest, TENANT_ID)
      .compose(migrationJob -> asyncMigrationJobService.getById(migrationJob.getId(), TENANT_ID))
      .onComplete(testContext.succeeding(migrationJobOptional -> testContext.verify(() -> {
        assertTrue(migrationJobOptional.isPresent());
        assertEquals(ERROR, migrationJobOptional.get().getStatus());
        testContext.completeNow();
      })));
  }

}
