package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.Item;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordBatch;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(VertxUnitRunner.class)
public class RecordApiTest extends AbstractRestVerticleTest {

  static final String SOURCE_STORAGE_SOURCE_RECORDS_PATH = "/source-storage/sourceRecords";
  private static final String SOURCE_STORAGE_RECORDS_PATH = "/source-storage/records";
  private static final String CREATE_BATCH_RECORDS_PATH = "/source-storage/batch/records/create";
  private static final String BATCH_PARSED_RECORDS_PATH = "/source-storage/batch/parsed-records";
  private static final String SOURCE_STORAGE_SNAPSHOTS_PATH = "/source-storage/snapshots";
  private static final String SNAPSHOTS_TABLE_NAME = "snapshots";
  private static final String RECORDS_TABLE_NAME = "records";
  private static final String RAW_RECORDS_TABLE_NAME = "raw_records";
  private static final String ERROR_RECORDS_TABLE_NAME = "error_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";

  private static RawRecord rawRecord;
  private static ParsedRecord marcRecord;

  static {
    try {
      rawRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class));
      marcRecord = new ParsedRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static ParsedRecord invalidParsedRecord = new ParsedRecord()
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  private static ErrorRecord errorRecord = new ErrorRecord()
    .withDescription("Oops... something happened")
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
  private static Snapshot snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Record record_1 = new Record()
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_2 = new Record()
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_3 = new Record()
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_4 = new Record()
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_5 = new Record()
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withMatchedId(UUID.randomUUID().toString())
    .withParsedRecord(invalidParsedRecord);

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(RECORDS_TABLE_NAME, new Criterion(), event -> {
      pgClient.delete(RAW_RECORDS_TABLE_NAME, new Criterion(), event1 -> {
        pgClient.delete(ERROR_RECORDS_TABLE_NAME, new Criterion(), event2 -> {
          pgClient.delete(MARC_RECORDS_TABLE_NAME, new Criterion(), event3 -> {
            pgClient.delete(SNAPSHOTS_TABLE_NAME, new Criterion(), event4 -> {
              if (event4.failed()) {
                context.fail(event4.cause());
              }
              async.complete();
            });
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
      .get(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("records", empty());
  }

  @Test
  public void shouldReturnAllRecordsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_1, record_2, record_3);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(recordsToPost.size()));
    async.complete();
  }

  @Test
  public void shouldReturnRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_1, record_2, record_3);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?query=snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("records*.snapshotId", everyItem(is(record_2.getSnapshotId())))
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnLimitedCollectionOnGetWithLimit(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_1, record_2, record_3);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(2))
      .body("totalRecords", is(recordsToPost.size()));
    async.complete();
  }

  @Test
  public void shouldReturnSortedSourceRecordsOnGetWhenSortByIsSpecified(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_2, record_2, record_4, record_4);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=(recordType==\"MARC\") sortBy metadata.createdDate/sort.descending")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(4))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    Assert.assertTrue(sourceRecordList.get(0).getMetadata().getCreatedDate().after(sourceRecordList.get(1).getMetadata().getCreatedDate()));
    Assert.assertTrue(sourceRecordList.get(1).getMetadata().getCreatedDate().after(sourceRecordList.get(2).getMetadata().getCreatedDate()));
    Assert.assertTrue(sourceRecordList.get(2).getMetadata().getCreatedDate().after(sourceRecordList.get(3).getMetadata().getCreatedDate()));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoRecordPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateRecordOnPost(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_1.getSnapshotId()))
      .body("recordType", is(record_1.getRecordType().name()))
      .body("rawRecord.content", is(rawRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordOnPost(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_3.getSnapshotId()))
      .body("recordType", is(record_3.getRecordType().name()))
      .body("rawRecord.content", is(rawRecord.getContent()))
      .body("errorRecord.content", is(errorRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoRecordPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    createdRecord.setParsedRecord(marcRecord);
    Response putResponse = RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(putResponse.statusCode(), is(HttpStatus.SC_OK));
    Record updatedRecord = putResponse.body().as(Record.class);
    assertThat(updatedRecord.getId(), is(createdRecord.getId()));
    assertThat(updatedRecord.getRawRecord().getContent(), is(rawRecord.getContent()));
    ParsedRecord parsedRecord = updatedRecord.getParsedRecord();
    assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(updatedRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldUpdateErrorRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    createdRecord.setErrorRecord(errorRecord);
    RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(createdRecord.getId()))
      .body("rawRecord.content", is(createdRecord.getRawRecord().getContent()))
      .body("errorRecord.content", is(createdRecord.getErrorRecord().getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingRecordOnGetById(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(createdRecord.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawRecord.getContent()));
    ParsedRecord parsedRecord = getRecord.getParsedRecord();
    assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/11dfac11-1caf-4470-9ad1-d533f6360bdd")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingRecordOnDelete(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("deleted", is(true));
    async.complete();

    async = testContext.async();
    Response createErrorRecord = RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createErrorRecord.statusCode(), is(HttpStatus.SC_CREATED));
    Record errorRecord = createErrorRecord.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + errorRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + errorRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("deleted", is(true));
    async.complete();
  }

  @Test
  public void shouldReturnEmptyListOnGetResultsIfNoRecordsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("sourceRecords", empty());
  }

  @Test
  public void shouldReturnAllParsedResultsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_1, record_2, record_3, record_4);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("sourceRecords*.parsedRecord", notNullValue())
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnResultsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_1, record_2, record_3, record_4);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("sourceRecords*.snapshotId", everyItem(is(record_2.getSnapshotId())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .body("sourceRecords*.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnLimitedResultCollectionOnGetWithLimit(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = asList(snapshot_1, snapshot_2);
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
    List<Record> recordsToPost = asList(record_1, record_2, record_3, record_4);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?limit=1")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", greaterThanOrEqualTo(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnAllSourceRecordsMarkedAsDeletedOnGetWhenParameterDeletedIsTrue(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsedRecord = createParsed.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsedRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    createParsed = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    parsedRecord = createParsed.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsedRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .param("deleted", true)
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", greaterThanOrEqualTo(2))
      .body("sourceRecords*.deleted", everyItem(is(true)));
    async.complete();
  }

  @Test
  public void shouldReturnOnlyUnmarkedAsDeletedSourceRecordOnGetWhenParameterDeletedIsNotPassed(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(is(HttpStatus.SC_CREATED));
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record recordToDelete = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + recordToDelete.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", greaterThanOrEqualTo(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordIfParsedContentIsInvalid(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_5)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(createdRecord.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawRecord.getContent()));
    assertThat(getRecord.getParsedRecord(), nullValue());
    assertThat(getRecord.getErrorRecord(), notNullValue());
    Assert.assertFalse(getRecord.getDeleted());
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=error!")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=select * from table")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?query=error!")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?query=select * from table")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnCreatedRecordWithAdditionalInfoOnGetById(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    Record newRecord = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(true));

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(createdRecord.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawRecord.getContent()));
    ParsedRecord parsedRecord = getRecord.getParsedRecord();
    assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(newRecord.getAdditionalInfo().getSuppressDiscovery()));
    async.complete();
  }

  @Test
  public void shouldReturnSourceRecordWithAdditionalInfoOnGetBySpecifiedSnapshotId(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    Record newRecord = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(true));

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=snapshotId=" + newRecord.getSnapshotId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    SourceRecordCollection sourceRecordCollection = getResponse.body().as(SourceRecordCollection.class);
    assertThat(sourceRecordCollection.getSourceRecords().size(), is(1));
    SourceRecord sourceRecord = sourceRecordCollection.getSourceRecords().get(0);
    assertThat(sourceRecord.getRecordId(), is(createdRecord.getId()));
    assertThat(sourceRecord.getRawRecord().getContent(), is(rawRecord.getContent()));
    assertThat(sourceRecord.getAdditionalInfo().getSuppressDiscovery(), is(createdRecord.getAdditionalInfo().getSuppressDiscovery()));
    async.complete();
  }

  @Test
  public void shouldCreateBatchOfRecords(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();


    RecordBatch batch = new RecordBatch()
      .withItems(asList(new Item().withRecord(record_1), new Item().withRecord(record_4)));

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(batch)
      .when()
      .post(CREATE_BATCH_RECORDS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_MULTI_STATUS)
      .body("items*.status", everyItem(is(200)))
      .body("items*.href", notNullValue())
      .body("items*.record.snapshotId", everyItem(is(snapshot_1.getJobExecutionId())))
      .body("items*.record.recordType", everyItem(is(record_1.getRecordType().name())))
      .body("items*.record.rawRecord.content", notNullValue())
      .body("items*.record.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Ignore("Validation doesn't work")
  @Test
  public void shouldReturnBadRequestOnPostWhenNoRecordsInRecordCollection() {
    RestAssured.given()
      .spec(spec)
      .body(new RecordBatch())
      .when()
      .post(CREATE_BATCH_RECORDS_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateRawRecordAndErrorRecordOnPostThemInRecordCollection(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    RecordBatch recordBatch = new RecordBatch()
      .withItems(asList(new Item().withRecord(record_2), new Item().withRecord(record_3)));


    async = testContext.async();
    RecordBatch createdRecords = RestAssured.given()
      .spec(spec)
      .body(recordBatch)
      .when()
      .post(CREATE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_MULTI_STATUS)
      .extract().response().body().as(RecordBatch.class);

    Item item = createdRecords.getItems().get(0);
    Record createdRecord = item.getRecord();
    assertThat(item.getStatus(), is(200));
    assertThat(createdRecord.getId(), notNullValue());
    assertThat(createdRecord.getSnapshotId(), is(record_2.getSnapshotId()));
    assertThat(createdRecord.getRecordType(), is(record_2.getRecordType()));
    assertThat(createdRecord.getRawRecord().getContent(), is(record_2.getRawRecord().getContent()));
    assertThat(createdRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));

    item = createdRecords.getItems().get(1);
    assertThat(item.getStatus(), is(200));
    createdRecord = item.getRecord();
    assertThat(createdRecord.getId(), notNullValue());
    assertThat(createdRecord.getSnapshotId(), is(record_3.getSnapshotId()));
    assertThat(createdRecord.getRecordType(), is(record_3.getRecordType()));
    assertThat(createdRecord.getRawRecord().getContent(), is(record_3.getRawRecord().getContent()));
    assertThat(createdRecord.getErrorRecord().getContent(), is(record_3.getErrorRecord().getContent()));
    assertThat(createdRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldCreatePartOfRawRecordsOnBatchSaveOperation(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    RecordBatch recordBatch = new RecordBatch()
      .withItems(asList(new Item().withRecord(record_2), new Item().withRecord(record_4)));

    async = testContext.async();
    RecordBatch createdRecords = RestAssured.given()
      .spec(spec)
      .body(recordBatch)
      .when()
      .post(CREATE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_MULTI_STATUS)
      .extract().response().body().as(RecordBatch.class);

    assertThat(createdRecords.getItems().size(), is(2));

    Item item = createdRecords.getItems().get(0);
    Record createdRecord = createdRecords.getItems().get(0).getRecord();

    assertThat(item.getStatus(), is(200));

    assertThat(createdRecord.getId(), notNullValue());
    assertThat(createdRecord.getSnapshotId(), is(record_2.getSnapshotId()));
    assertThat(createdRecord.getRecordType(), is(record_2.getRecordType()));
    assertThat(createdRecord.getRawRecord().getContent(), is(record_2.getRawRecord().getContent()));
    assertThat(createdRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));

    Item failedItem = createdRecords.getItems().get(1);
    assertThat(failedItem.getStatus(), is(404));
    assertThat(failedItem.getErrorMessage(), is("Couldn't find snapshot with id " + snapshot_1.getJobExecutionId()));
    async.complete();
  }


  @Test
  public void shouldUpdateParsedRecords(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    Record newRecord = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(false));

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    ParsedRecordCollection parsedRecordCollection = new ParsedRecordCollection()
      .withRecordType(ParsedRecordCollection.RecordType.MARC)
      .withParsedRecords(Collections.singletonList(new ParsedRecord().withContent(marcRecord.getContent()).withId(createdRecord.getParsedRecord().getId())))
      .withTotalRecords(1);

    async = testContext.async();
    ParsedRecordCollection updatedParsedRecordCollection = RestAssured.given()
      .spec(spec)
      .body(parsedRecordCollection)
      .when()
      .put(BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(ParsedRecordCollection.class);

    ParsedRecord updatedParsedRecord = updatedParsedRecordCollection.getParsedRecords().get(0);
    assertThat(updatedParsedRecord.getId(), notNullValue());
    assertThat(JsonObject.mapFrom(updatedParsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnUpdateParsedRecordsIfNoIdPassed(TestContext testContext) {
    Async async = testContext.async();
    ParsedRecordCollection parsedRecordCollection = new ParsedRecordCollection()
      .withRecordType(ParsedRecordCollection.RecordType.MARC)
      .withParsedRecords(asList(new ParsedRecord().withContent(marcRecord.getContent()).withId(UUID.randomUUID().toString()),
        new ParsedRecord().withContent(marcRecord.getContent()).withId(null)))
      .withTotalRecords(2);

    RestAssured.given()
      .spec(spec)
      .body(parsedRecordCollection)
      .when()
      .put(BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldUpdateParsedRecordsWithJsonContent(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    Record newRecord = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(false));

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    ParsedRecord parsedRecordJson = new ParsedRecord().withId(createdRecord.getParsedRecord().getId())
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500").put("fields", new JsonArray()));

    ParsedRecordCollection parsedRecordCollection = new ParsedRecordCollection()
      .withRecordType(ParsedRecordCollection.RecordType.MARC)
      .withParsedRecords(Collections.singletonList(parsedRecordJson))
      .withTotalRecords(1);

    async = testContext.async();
    ParsedRecordCollection updatedParsedRecordCollection = RestAssured.given()
      .spec(spec)
      .body(parsedRecordCollection)
      .when()
      .put(BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(ParsedRecordCollection.class);

    ParsedRecord updatedParsedRecord = updatedParsedRecordCollection.getParsedRecords().get(0);
    assertThat(updatedParsedRecord.getId(), notNullValue());
    assertThat(JsonObject.mapFrom(updatedParsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    async.complete();
  }

  @Test
  public void shouldReturnErrorMessagesOnUpdateParsedRecordsIfIdIsNotFound(TestContext testContext) {
    Async async = testContext.async();
    ParsedRecordCollection parsedRecordCollection = new ParsedRecordCollection()
      .withRecordType(ParsedRecordCollection.RecordType.MARC)
      .withParsedRecords(asList(new ParsedRecord().withContent(marcRecord.getContent()).withId(UUID.randomUUID().toString()),
        new ParsedRecord().withContent(marcRecord.getContent()).withId(UUID.randomUUID().toString())))
      .withTotalRecords(2);

    ParsedRecordCollection result = RestAssured.given()
      .spec(spec)
      .body(parsedRecordCollection)
      .when()
      .put(BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .extract().response().body().as(ParsedRecordCollection.class);

    assertThat(result.getErrorMessages(), hasSize(2));
    async.complete();
  }


}
