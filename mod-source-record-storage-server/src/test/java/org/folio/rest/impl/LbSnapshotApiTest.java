package org.folio.rest.impl;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.http.HttpStatus;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.LbSnapshotDaoUtil;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LbSnapshotApiTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_SNAPSHOTS_PATH = "/lb-source-storage/snapshots";

  private static Snapshot snapshot_1 = new Snapshot()
    .withJobExecutionId("67dfac11-1caf-4470-9ad1-d533f6360bdd")
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_2 = new Snapshot()
    .withJobExecutionId("17dfac11-1caf-4470-9ad1-d533f6360bdd")
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_3 = new Snapshot()
    .withJobExecutionId("27dfac11-1caf-4470-9ad1-d533f6360bdd")
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot snapshot_4 = new Snapshot()
    .withJobExecutionId("37dfac11-1caf-4470-9ad1-d533f6360bdd")
    .withStatus(Snapshot.Status.PARSING_FINISHED);

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    LbSnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoSnapshotsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("snapshots", empty());
  }

  @Test
  public void shouldReturnAllSnapshotsOnGetWhenNoQueryIsSpecified() {
    List<Snapshot> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2, snapshot_3);
    for (Snapshot snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(snapshotsToPost.size()));
  }

  @Test
  public void shouldReturnNewSnapshotsOnGetByStatusNew(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2, snapshot_3);
    for (Snapshot snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?status=" + Snapshot.Status.NEW.name())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("snapshots*.status", everyItem(is(Snapshot.Status.NEW.name())));
    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?status=error!")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?orderBy=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnLimitedCollectionOnGet(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2, snapshot_3, snapshot_4);
    for (Snapshot snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot)
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?limit=3")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("snapshots.size()", is(3))
      .body("totalRecords", is(snapshotsToPost.size()));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateSnapshotOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_1.getJobExecutionId()))
      .body("status", is(snapshot_1.getStatus().name()));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingSnapshotOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()));
    async.complete();

    async = testContext.async();
    snapshot_4.setStatus(Snapshot.Status.COMMIT_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_4.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingSnapshotOnGetById(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_2.getJobExecutionId()))
      .body("status", is(snapshot_2.getStatus().name()));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_2.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_2.getJobExecutionId()))
      .body("status", is(snapshot_2.getStatus().name()));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_3.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingSnapshotOnDelete(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_3)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_3.getJobExecutionId()))
      .body("status", is(snapshot_3.getStatus().name()));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_3.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();
  }

  @Test
  public void shouldSetProcessingStartedDateOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_3)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_3.getJobExecutionId()))
      .body("status", is(snapshot_3.getStatus().name()))
      .body("processingStartedDate", notNullValue(Date.class));
  }

  @Test
  public void shouldSetProcessingStartedDateOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()))
      .body("processingStartedDate", nullValue(Date.class));
    async.complete();

    async = testContext.async();
    snapshot_4.setStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_4.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()))
      .body("processingStartedDate", notNullValue(Date.class));
    async.complete();
  }

}
