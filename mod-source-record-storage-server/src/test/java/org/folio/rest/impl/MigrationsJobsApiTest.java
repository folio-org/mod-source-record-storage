package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.Wait;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AsyncMigrationJob;
import org.folio.rest.jaxrs.model.AsyncMigrationJobInitRq;
import org.folio.rest.jooq.Tables;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.rest.jaxrs.model.AsyncMigrationJob.Status.COMPLETED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class MigrationsJobsApiTest extends AbstractRestVerticleTest {

  static final String MIGRATIONS_JOBS_PATH = "/source-storage/migrations/jobs/";

  @Before
  public void setUp(TestContext context) {
    super.setUp();
    clearTable(context);
  }

  @Test
  public void shouldExecuteAsyncMigration() throws InterruptedException {
    AsyncMigrationJobInitRq migrationInitDto = new AsyncMigrationJobInitRq()
      .withMigrations(List.of("marcIndexersVersionMigration"));

    AsyncMigrationJob migrationJob = RestAssured.given()
      .spec(spec)
      .body(migrationInitDto)
      .when()
      .post(MIGRATIONS_JOBS_PATH)
      .then()
      .statusCode(HttpStatus.SC_ACCEPTED)
      .body("id", notNullValue())
      .body("migrations", contains(migrationInitDto.getMigrations().toArray()))
      .body("status", is(AsyncMigrationJob.Status.IN_PROGRESS.value()))
      .body("startedDate", notNullValue())
      .extract().body().as(AsyncMigrationJob.class);

    Wait.delay(3);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(MIGRATIONS_JOBS_PATH + migrationJob.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(migrationJob.getId()))
      .body("status", is(COMPLETED.value()))
      .body("completedDate", notNullValue());
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdIfJobDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(MIGRATIONS_JOBS_PATH + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestOnPostIfSpecifiedMigrationIsNotSupported() {
    AsyncMigrationJobInitRq migrationInitDto = new AsyncMigrationJobInitRq()
      .withMigrations(List.of("unsupportedMigrationName"));

    RestAssured.given()
      .spec(spec)
      .body(migrationInitDto)
      .when()
      .post(MIGRATIONS_JOBS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST)
      .body(is(format("Specified migrations are not supported. Migrations: %s", migrationInitDto.getMigrations())));
  }

  @Test
  public void shouldReturnBadRequestOnPostIfOtherMigrationJobInProgress(TestContext testContext) throws InterruptedException {
    Async async = testContext.async();
    AsyncMigrationJobInitRq migrationInitDto = new AsyncMigrationJobInitRq()
      .withMigrations(List.of("marcIndexersVersionMigration"));

    // Trigger the first migration job
    String runningJobId = RestAssured.given()
      .spec(spec)
      .body(migrationInitDto)
      .when()
      .post(MIGRATIONS_JOBS_PATH)
      .then()
      .statusCode(HttpStatus.SC_ACCEPTED)
      .body("status", is(AsyncMigrationJob.Status.IN_PROGRESS.value()))
      .extract().body().as(AsyncMigrationJob.class).getId();

    System.out.println("First migration job initiated with ID: " + runningJobId);

    // Now trigger the second migration job and expect a conflict
    RestAssured.given()
      .spec(spec)
      .body(migrationInitDto)
      .when()
      .post(MIGRATIONS_JOBS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CONFLICT)
      .body(is(format("Failed to initiate migration job, because migration job with id '%s' already in progress", runningJobId)));

    // Wait for the running job to complete before finishing the test
    System.out.println("Waiting for the first migration job to complete");
    await().atMost(10, SECONDS).until(() -> {
      String status = RestAssured.given()
        .spec(spec)
        .when()
        .get(MIGRATIONS_JOBS_PATH + runningJobId)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().jsonPath().getString("status");
      return status.equals(AsyncMigrationJob.Status.COMPLETED.value());
    });
    async.complete();
  }

  private void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx.getDelegate(), TENANT_ID);
    pgClient.delete(Tables.ASYNC_MIGRATION_JOBS.getName(), new Criterion(), ar -> {
      if (ar.failed()) {
        context.fail(ar.cause());
      }
      async.complete();
    });
  }

}
