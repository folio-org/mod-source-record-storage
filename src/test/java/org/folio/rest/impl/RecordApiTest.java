package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@RunWith(VertxUnitRunner.class)
public class RecordApiTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_RECORD_PATH = "/source-storage/record";
  private static final String SOURCE_STORAGE_RESULT_PATH = "/source-storage/result";
  private static final String RECORDS_TABLE_NAME = "records";
  private static final String SOURCE_RECORDS_TABLE_NAME = "source_records";
  private static final String ERROR_RECORDS_TABLE_NAME = "error_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";

  private static JsonObject sourceRecord_1 =  new JsonObject()
    .put("source", "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.");
  private static JsonObject sourceRecord_2 = new JsonObject()
    .put("source", "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
  private static JsonObject marcRecord = new JsonObject()
    .put("content", "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.");
  private static JsonObject errorRecord = new JsonObject()
    .put("description", "Oops... something happened")
    .put("content", "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  private static JsonObject record_1 = new JsonObject()
    .put("snapshotId", "11dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("recordType", "MARC")
    .put("sourceRecord", sourceRecord_1);
  private static JsonObject record_2 = new JsonObject()
    .put("snapshotId", "22dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("recordType", "MARC")
    .put("sourceRecord", sourceRecord_2)
    .put("parsedRecord", marcRecord);
  private static JsonObject record_3 = new JsonObject()
    .put("snapshotId", "22dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("recordType", "MARC")
    .put("sourceRecord", sourceRecord_1)
    .put("errorRecord", errorRecord);
  private static JsonObject record_4 = new JsonObject()
    .put("snapshotId", "11dfac11-1caf-4470-9ad1-d533f6360bdd")
    .put("recordType", "MARC")
    .put("sourceRecord", sourceRecord_1)
    .put("parsedRecord", marcRecord);

  @Override
  public void clearTables(TestContext context) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(RECORDS_TABLE_NAME, new Criterion(), event -> {
      pgClient.delete(SOURCE_RECORDS_TABLE_NAME, new Criterion(), event1 -> {
        pgClient.delete(ERROR_RECORDS_TABLE_NAME, new Criterion(), event2 -> {
          pgClient.delete(MARC_RECORDS_TABLE_NAME, new Criterion(), event3 -> {
            if (event3.failed()) {
              context.fail(event3.cause());
            }
          });
        });
      });
    });
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoRecordsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("records", empty());
  }

  @Test
  public void shouldReturnAllRecordsOnGetWhenNoQueryIsSpecified() {
    List<JsonObject> recordsToPost = Arrays.asList(record_1, record_2, record_3);
    for (JsonObject record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record.toString())
        .when()
        .post(SOURCE_STORAGE_RECORD_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(recordsToPost.size()));
  }

  @Test
  public void shouldReturnRecordsOnGetBySpecifiedSnapshotId() {
    List<JsonObject> recordsToPost = Arrays.asList(record_1, record_2, record_3);
    for (JsonObject record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record.toString())
        .when()
        .post(SOURCE_STORAGE_RECORD_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORD_PATH + "?query=snapshotId=" + record_2.getString("snapshotId"))
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("records*.snapshotId", everyItem(is(record_2.getString("snapshotId"))));
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoRecordPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateRecordOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(record_1.toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_1.getString("snapshotId")))
      .body("recordType", is(record_1.getString("recordType")))
      .body("sourceRecord.source", is(sourceRecord_1.getString("source")));
  }

  @Test
  public void shouldCreateErrorRecordOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(record_3.toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_3.getString("snapshotId")))
      .body("recordType", is(record_3.getString("recordType")))
      .body("sourceRecord.source", is(sourceRecord_1.getString("source")))
      .body("errorRecord.content", is(errorRecord.getString("content")));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoRecordPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_RECORD_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(record_1.toString())
      .when()
      .put(SOURCE_STORAGE_RECORD_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingRecordOnPut() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1.toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    createdRecord.setParsedRecord(marcRecord.mapTo(ParsedRecord.class));
    RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORD_PATH + "/" + createdRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdRecord.getId()))
      .body("sourceRecord.source", is(createdRecord.getSourceRecord().getSource()))
      .body("parsedRecord.content", is(createdRecord.getParsedRecord().getContent()));
  }

  @Test
  public void shouldUpdateErrorRecordOnPut() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1.toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    createdRecord.setErrorRecord(errorRecord.mapTo(ErrorRecord.class));
    RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORD_PATH + "/" + createdRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdRecord.getId()))
      .body("sourceRecord.source", is(createdRecord.getSourceRecord().getSource()))
      .body("errorRecord.content", is(createdRecord.getErrorRecord().getContent()));
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORD_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingRecordOnGetById() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_2.toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORD_PATH + "/" + createdRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdRecord.getId()))
      .body("sourceRecord.source", is(sourceRecord_2.getString("source")))
      .body("parsedRecord.content", is(marcRecord.getString("content")));
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORD_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingRecordOnDelete() {
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1.toString())
      .when()
      .post(SOURCE_STORAGE_RECORD_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORD_PATH + "/" + createdRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void shouldReturnEmptyListOnGetResultsIfNoRecordsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RESULT_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("results", empty());
  }

  @Test
  public void shouldReturnAllParsedResultsOnGetWhenNoQueryIsSpecified() {
    List<JsonObject> recordsToPost = Arrays.asList(record_1, record_2, record_3, record_4);
    for (JsonObject record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record.toString())
        .when()
        .post(SOURCE_STORAGE_RECORD_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RESULT_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("results*.parsedRecord", notNullValue());
  }

  @Test
  public void shouldReturnResultsOnGetBySpecifiedSnapshotId() {
    List<JsonObject> recordsToPost = Arrays.asList(record_1, record_2, record_3, record_4);
    for (JsonObject record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record.toString())
        .when()
        .post(SOURCE_STORAGE_RECORD_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RESULT_PATH + "?query=snapshotId=" + record_2.getString("snapshotId"))
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("results*.snapshotId", everyItem(is(record_2.getString("snapshotId"))));
  }

}
