package org.folio.rest.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.restassured.RestAssured;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SourceStorageStreamApiTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_STREAM_RECORDS_PATH = "/source-storage/stream/records";

  private static final String FIRST_UUID = UUID.randomUUID().toString();
  private static final String SECOND_UUID = UUID.randomUUID().toString();
  private static final String THIRD_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_UUID = UUID.randomUUID().toString();

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
    .withId(FIRST_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withMatchedId(FIRST_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record record_2 = new Record()
    .withId(SECOND_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(SECOND_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL);
  private static Record record_3 = new Record()
    .withId(THIRD_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_UUID)
    .withState(Record.State.ACTUAL);

  @Before
  public void setUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoRecordsExist(TestContext testContext) {
    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();
    List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(0, actual.size());
        async.complete();
      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
  }

  @Test
  public void shouldReturnAllRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2);
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

    Record record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    List<Record> recordsToPost = Arrays.asList(record_1, record_2, record_3, record_4);
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

    final Async finalAsync = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

      List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(4, actual.size());
        finalAsync.complete();
      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
  }

  @Test
  public void shouldReturnRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2);
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

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    List<Record> recordsToPost = Arrays.asList(record_1, record_2, record_3, recordWithOldStatus);
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

    final Async finalAsync = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH + "?state=ACTUAL&snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

      List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(2, actual.size());
        testContext.assertEquals(record_2.getSnapshotId(), actual.get(0).getSnapshotId());
        testContext.assertEquals(record_2.getSnapshotId(), actual.get(1).getSnapshotId());
        testContext.assertEquals(false, actual.get(1).getAdditionalInfo().getSuppressDiscovery());
        testContext.assertEquals(false, actual.get(1).getAdditionalInfo().getSuppressDiscovery());
        finalAsync.complete();
      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
  }

  @Test
  public void shouldReturnLimitedCollectionWithActualStateOnGetWithLimit(TestContext testContext) {
    Async async = testContext.async();
    List<Snapshot> snapshotsToPost = Arrays.asList(snapshot_1, snapshot_2);
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

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    List<Record> recordsToPost = Arrays.asList(record_1, record_2, record_3, recordWithOldStatus);
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

    final Async finalAsync = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH + "?state=ACTUAL&limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(2, actual.size());
        finalAsync.complete();
      }).collect(() -> actual, (a, r) -> a.add(r))
        .subscribe();
  }

  private Flowable<String> flowableInputStreamScanner(InputStream inputStream) {
    return Flowable.create(subscriber -> {
      try (Scanner scanner = new Scanner(inputStream, "UTF-8")) {
        while (scanner.hasNext()) {
          subscriber.onNext(scanner.nextLine());
        }
      }
      subscriber.onComplete();
    }, BackpressureStrategy.BUFFER);
  }

}
