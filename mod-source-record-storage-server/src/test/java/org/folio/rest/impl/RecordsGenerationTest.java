package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class RecordsGenerationTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_RECORDS_PATH = "/source-storage/records";
  private static final String SOURCE_STORAGE_SNAPSHOTS_PATH = "/source-storage/snapshots";
  private static final String SNAPSHOTS_TABLE_NAME = "snapshots";
  private static final String RECORDS_TABLE_NAME = "records";
  private static final String RAW_RECORDS_TABLE_NAME = "raw_records";
  private static final String ERROR_RECORDS_TABLE_NAME = "error_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";

  private static String matchedId = UUID.randomUUID().toString();
  private static RawRecord rawRecord = new RawRecord()
    .withContent("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.");
  private static ParsedRecord marcRecord = new ParsedRecord()
    .withContent("Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.");
  private static Snapshot snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_4 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.NEW);

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
  public void shouldCalculateRecordsGeneration() {
    List<Snapshot> snapshots = Arrays.asList(snapshot_1, snapshot_2, snapshot_3, snapshot_4);
    for (int i = 0; i < snapshots.size(); i++) {
      RestAssured.given()
        .spec(spec)
        .body(snapshots.get(i).withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);

      Record record = new Record()
        .withSnapshotId(snapshots.get(i).getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withRawRecord(rawRecord)
        .withParsedRecord(marcRecord)
        .withMatchedId(matchedId);

      Record created = RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);

      snapshots.get(i).setStatus(Snapshot.Status.COMMITTED);
      RestAssured.given()
        .spec(spec)
        .body(snapshots.get(i))
        .when()
        .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshots.get(i).getJobExecutionId())
        .then()
        .statusCode(HttpStatus.SC_OK);

      RestAssured.given()
        .spec(spec)
        .when()
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + created.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(created.getId()))
        .body("rawRecord.content", is(rawRecord.getContent()))
        .body("parsedRecord.content", is(marcRecord.getContent()))
        .body("matchedId", is(matchedId))
        .body("generation", is(i));
    }
  }

  @Test
  public void shouldNotUpdateRecordsGenerationIfSnapshotsNotCommitted() {
    List<Snapshot> snapshots = Arrays.asList(snapshot_1, snapshot_2, snapshot_3, snapshot_4);
    for (Snapshot snapshot : snapshots) {
      RestAssured.given()
        .spec(spec)
        .body(snapshot.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);

      Record record = new Record()
        .withSnapshotId(snapshot.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withRawRecord(rawRecord)
        .withParsedRecord(marcRecord)
        .withMatchedId(matchedId);

      Record created = RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);

      RestAssured.given()
        .spec(spec)
        .when()
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + created.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(created.getId()))
        .body("rawRecord.content", is(rawRecord.getContent()))
        .body("parsedRecord.content", is(marcRecord.getContent()))
        .body("matchedId", is(matchedId))
        .body("generation", is(0));
    }
  }

  @Test
  public void shouldNotUpdateRecordsGenerationIfSnapshotsCommittedAfter() {
    List<Snapshot> snapshots = Arrays.asList(snapshot_1, snapshot_2);
    for (int i = 0; i < snapshots.size(); i++) {
      RestAssured.given()
        .spec(spec)
        .body(snapshots.get(i).withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);

      Record record = new Record()
        .withSnapshotId(snapshots.get(i).getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withRawRecord(rawRecord)
        .withParsedRecord(marcRecord)
        .withMatchedId(matchedId);

      if (i > 0) {
        snapshots.get(i - 1).setStatus(Snapshot.Status.COMMITTED);
        RestAssured.given()
          .spec(spec)
          .body(snapshots.get(i - 1))
          .when()
          .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshots.get(i - 1).getJobExecutionId())
          .then()
          .statusCode(HttpStatus.SC_OK);
      }

      Record created = RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);

      RestAssured.given()
        .spec(spec)
        .when()
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + created.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(created.getId()))
        .body("rawRecord.content", is(rawRecord.getContent()))
        .body("parsedRecord.content", is(marcRecord.getContent()))
        .body("matchedId", is(matchedId))
        .body("generation", is(0));
    }
  }

  @Test
  public void shouldReturnNotFoundIfSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);

    Record record_1 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId);

    RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);

    Record record_2 = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId);

    RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnBadRequestIfProcessingDateIsNull() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.NEW))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);

    Record record_1 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId);

    RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }
}
