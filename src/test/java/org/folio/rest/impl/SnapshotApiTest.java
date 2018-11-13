package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class SnapshotApiTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_SNAPSHOT_PATH = "/source-storage/snapshot";
  private static final String SNAPSHOTS_TABLE_NAME = "snapshots";

  private static JsonObject snapshot_1 = new JsonObject()
    .put("jobExecutionId", "67dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("status", "NEW");
  private static JsonObject snapshot_2 = new JsonObject()
    .put("jobExecutionId", "17dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("status", "NEW");
  private static JsonObject snapshot_3 = new JsonObject()
    .put("jobExecutionId", "27dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("status", "PARSING_IN_PROGRESS");
  private static JsonObject snapshot_4 = new JsonObject()
    .put("jobExecutionId", "37dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("status", "IMPORT_IN_PROGRESS");


  @Override
  public void clearTables(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT_ID).delete(SNAPSHOTS_TABLE_NAME, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoSnapshotsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("snapshots", empty());
  }

  @Test
  public void shouldReturnAllSnapshotsOnGetWhenNoQueryIsSpecified() {
    List<JsonObject> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2, snapshot_3);
    for (JsonObject snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot.toString())
        .when()
        .post(SOURCE_STORAGE_SNAPSHOT_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    Object[] ids = snapshotsToPost.stream().map(r -> r.getString("jobExecutionId")).toArray();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(snapshotsToPost.size()))
      .body("snapshots*.jobExecutionId", contains(ids));
  }

  @Test
  public void shouldReturnNewSnapshotsOnGetByStatusNew() {
    List<JsonObject> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2, snapshot_3);
    for (JsonObject snapshot : snapshotsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot.toString())
        .when()
        .post(SOURCE_STORAGE_SNAPSHOT_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOT_PATH + "?query=status=NEW")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("snapshots*.status", everyItem(is("NEW")));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateSnapshotOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_1.getString("jobExecutionId")))
      .body("status", is(snapshot_1.getString("status")));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_1.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.toString())
      .when()
      .put(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_1.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingSnapshotOnPut() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4.toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_4.getString("jobExecutionId")))
      .body("status", is(snapshot_4.getString("status")));

    snapshot_4.put("status", "IMPORT_FINISHED");
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4.toString())
      .when()
      .put(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_4.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_4.getString("jobExecutionId")))
      .body("status", is(snapshot_4.getString("status")));
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_1.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingSnapshotOnGetById() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2.toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_2.getString("jobExecutionId")))
      .body("status", is(snapshot_2.getString("status")));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_2.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_2.getString("jobExecutionId")))
      .body("status", is(snapshot_2.getString("status")));
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_3.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingSnapshotOnDelete() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_3.toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOT_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_3.getString("jobExecutionId")))
      .body("status", is(snapshot_3.getString("status")));

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOT_PATH + "/" + snapshot_3.getString("jobExecutionId"))
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

}
