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
import static org.folio.rest.jaxrs.model.AsyncMigrationJob.Status.COMPLETED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
  public void shouldReturnBadRequestOnPostIfOtherMigrationJobInProgress() throws InterruptedException {
    AsyncMigrationJobInitRq migrationInitDto = new AsyncMigrationJobInitRq()
      .withMigrations(List.of("marcIndexersVersionMigration"));

    String runningJobId = RestAssured.given()
      .spec(spec)
      .body(migrationInitDto)
      .when()
      .post(MIGRATIONS_JOBS_PATH)
      .then()
      .statusCode(HttpStatus.SC_ACCEPTED)
      .body("status", is(AsyncMigrationJob.Status.IN_PROGRESS.value()))
      .extract().body().as(AsyncMigrationJob.class).getId();

    RestAssured.given()
      .spec(spec)
      .body(migrationInitDto)
      .when()
      .post(MIGRATIONS_JOBS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CONFLICT)
      .body(is(format("Failed to initiate migration job, because migration job with id '%s' already in progress", runningJobId)));

    // waits for running job completion before finishing the test to avoid impact on other tests
    Wait.delay(3);
    RestAssured.given()
      .spec(spec)
      .when()
      .get(MIGRATIONS_JOBS_PATH + runningJobId)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(runningJobId))
      .body("status", is(COMPLETED.value()));
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
