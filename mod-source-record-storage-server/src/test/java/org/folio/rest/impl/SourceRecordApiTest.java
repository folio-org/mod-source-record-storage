package org.folio.rest.impl;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@Ignore
@RunWith(VertxUnitRunner.class)
public class SourceRecordApiTest extends AbstractRestVerticleTest {

  private static final String FIRST_UUID = UUID.randomUUID().toString();
  private static final String SECOND_UUID = UUID.randomUUID().toString();
  private static final String THIRD_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_UUID = UUID.randomUUID().toString();
  private static final String SIXTH_UUID = UUID.randomUUID().toString();

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
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid("12345"));
  private static Record record_3 = new Record()
    .withId(THIRD_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_UUID)
    .withState(Record.State.ACTUAL);
  private static Record record_4 = new Record()
    .withId(FOURTH_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(FOURTH_UUID)
    .withOrder(1)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid("12345"));
  private static Record record_5 = new Record()
    .withId(FIFTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withMatchedId(FIFTH_UUID)
    .withParsedRecord(invalidParsedRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL);
  private static Record record_6 = new Record()
    .withId(SIXTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withMatchedId(SIXTH_UUID)
    .withParsedRecord(marcRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid("12345"));

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
  public void shouldReturnSpecificSourceRecordOnGetByRecordId(TestContext testContext) {
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
    List<Record> recordsToPost = Arrays.asList(record_1, record_3);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    Record createdRecord =
      RestAssured.given()
        .spec(spec)
        .body(record_2)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
    async.complete();
  }

  @Test
  public void shouldReturnSpecificSourceRecordOnGetByDefaultExternalId(TestContext testContext) {
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

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + FIRST_UUID)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(FIRST_UUID))
      .body("externalIdsHolder.instanceId", is(SECOND_UUID));
    async.complete();
  }

  @Test
  public void shouldReturnSpecificSourceRecordOnGetByInstanceExternalId(TestContext testContext) {
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
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String instanceId = UUID.randomUUID().toString();

    Record recordWithOldState = new Record().withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(recordWithOldState)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + instanceId + "?idType=INSTANCE")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(THIRD_UUID))
      .body("externalIdsHolder.instanceId", is(instanceId));
    async.complete();
  }

  @Test
  public void shouldReturnSpecificNumberOfSourceRecordsOnGetByInstanceExternalHrid(TestContext testContext) {
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

    String firstHrid = "123";
    String secondHrid = "1234";
    String thirdHrid = "1235";

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(firstHrid));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(secondHrid));

    RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    Record recordWithOldState = new Record().withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(THIRD_UUID).withInstanceHrid(thirdHrid));

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(secondHrid));

    RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(recordWithOldState)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?instanceHrid=" + secondHrid)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.externalIdsHolder.instanceHrid", everyItem(is(secondHrid)));
    async.complete();
  }

  @Test
  public void shouldReturnSpecificSourceRecordOnGetByRecordExternalId(TestContext testContext) {
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
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    Record recordWithOldState = new Record().withId(FIFTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIFTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(recordWithOldState)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String instanceId = UUID.randomUUID().toString();

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + SECOND_UUID + "?idType=RECORD")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(SECOND_UUID))
      .body("externalIdsHolder.instanceId", is(FIRST_UUID));
    async.complete();
  }

  //TODO fix
  @Ignore
  @Test
  public void shouldReturnSpecificSourceRecordOnGetByRecordLeaderRecordStatus(TestContext testContext) {
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
    List<Record> recordsToPost = Arrays.asList(record_1, record_3);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    Record createdRecord =
      RestAssured.given()
        .spec(spec)
        .body(record_2)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);
    async.complete();

    String leaderStatus = ParsedRecordDaoUtil.getLeaderStatus(createdRecord.getParsedRecord());

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?leaderRecordStatus=" + leaderStatus + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
    async.complete();
  }

  @Test
  public void shouldReturnSpecificSourceRecordOnGetIfInvalidExternalIdType(TestContext testContext) {
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
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String instanceId = UUID.randomUUID().toString();

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + SECOND_UUID + "?idType=invalidrecordtype")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(SECOND_UUID))
      .body("externalIdsHolder.instanceId", is(FIRST_UUID));
    async.complete();
  }

  @Test
  public void shouldNotReturnSpecificSourceRecordOnGetIfItIsNotExists(TestContext testContext) {
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
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String instanceId = UUID.randomUUID().toString();

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + FIFTH_UUID + "?idType=INSTANCE")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdIfParsedRecordIsNull(TestContext testContext) {
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
    List<Record> recordsToPost = Arrays.asList(record_1, record_3);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    Record createdRecord =
      RestAssured.given()
        .spec(spec)
        .body(record_3)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
    async.complete();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdIfThereISNoSuchRecord(TestContext testContext) {
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
    List<Record> recordsToPost = Arrays.asList(record_1, record_2, record_3);
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
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + UUID.randomUUID().toString() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
    async.complete();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdAndRecordStateActualIfRecordWasDeleted(TestContext testContext) {
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
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + parsed.getId() + "&recordState=ACTUAL&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGetByRecordIdIfInvalidUUID(TestContext testContext) {
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
    List<Record> recordsToPost = Arrays.asList(record_1, record_2);
    for (Record record : recordsToPost) {
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    Record createdRecord =
      RestAssured.given()
        .spec(spec)
        .body(record_3)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId().substring(1).replace("-", "") + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldReturnSortedSourceRecordsOnGetWhenSortByIsSpecified(TestContext testContext) {
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

    String firstMatchedId = UUID.randomUUID().toString();

    Record record_4_tmp = new Record()
      .withId(firstMatchedId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(firstMatchedId)
      .withOrder(1)
      .withState(Record.State.ACTUAL);

    String secondMathcedId = UUID.randomUUID().toString();

    Record record_2_tmp = new Record()
      .withId(secondMathcedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(secondMathcedId)
      .withOrder(11)
      .withState(Record.State.ACTUAL);

    List<Record> recordsToPost = Arrays.asList(record_2, record_2_tmp, record_4, record_4_tmp);
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
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=MARC&orderBy=createdDate,DESC")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(4))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertTrue(sourceRecordList.get(0).getMetadata().getCreatedDate().after(sourceRecordList.get(1).getMetadata().getCreatedDate()));
    testContext.assertTrue(sourceRecordList.get(1).getMetadata().getCreatedDate().after(sourceRecordList.get(2).getMetadata().getCreatedDate()));
    testContext.assertTrue(sourceRecordList.get(2).getMetadata().getCreatedDate().after(sourceRecordList.get(3).getMetadata().getCreatedDate()));
    async.complete();
  }

  @Test
  public void shouldReturnSortedSourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
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
    // NOTE: record_5 saves but fails parsed record content validation and does not save parsed record
    List<Record> recordsToPost = Arrays.asList(record_2, record_3, record_5, record_6);
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
    // NOTE: get source records will not return if there is no associated parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertEquals(11, sourceRecordList.get(0).getOrder().intValue());
    testContext.assertEquals(101, sourceRecordList.get(1).getOrder().intValue());
    async.complete();
  }

  @Test
  public void shouldReturnSourceRecordsForPeriod(TestContext testContext) {
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
    RestAssured.given()
        .spec(spec)
        .body(record_1)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    Date fromDate = new Date();
    String from = dateTimeFormatter.format(ZonedDateTime.ofInstant(fromDate.toInstant(), ZoneId.systemDefault()));

    async = testContext.async();
    // NOTE: record_5 saves but fails parsed record content validation and does not save parsed record
    List<Record> recordsToPost = Arrays.asList(record_2, record_3, record_4, record_5);
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

    Date toDate = new Date();
    String to = dateTimeFormatter.format(ZonedDateTime.ofInstant(toDate.toInstant(), ZoneId.systemDefault()));

    async = testContext.async();
    RestAssured.given()
        .spec(spec)
        .body(record_6)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    // NOTE: we do not expect record_3 or record_5 as they do not have a parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + from + "&updatedBefore=" + to)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertTrue(sourceRecordList.get(0).getMetadata().getUpdatedDate().after(fromDate));
    testContext.assertTrue(sourceRecordList.get(1).getMetadata().getUpdatedDate().after(fromDate));
    testContext.assertTrue(sourceRecordList.get(0).getMetadata().getUpdatedDate().before(toDate));
    testContext.assertTrue(sourceRecordList.get(1).getMetadata().getUpdatedDate().before(toDate));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + from)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(3))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + to)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();

    // NOTE: we do not expect record_1 id does not have a parsed record
    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedBefore=" + to)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedBefore=" + from)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnSourceRecordsByListOfId(TestContext testContext) {
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

    String firstSrsId = UUID.randomUUID().toString();
    String firstInstanceId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord().withId(firstSrsId)
      .withContent(new JsonObject().put("leader", "01542dcm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields", new JsonArray().add(new JsonObject().put("s", firstSrsId)).add(new JsonObject().put("i", firstInstanceId)))))));

    Record deleted_record_1 = new Record()
      .withId(firstSrsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(firstSrsId)
      .withLeaderRecordStatus("d")
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(firstInstanceId));

    String secondSrsId = UUID.randomUUID().toString();
    String secondInstanceId = UUID.randomUUID().toString();

    Record deleted_record_2 = new Record()
        .withId(secondSrsId)
        .withSnapshotId(snapshot_2.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withRawRecord(rawRecord)
        .withParsedRecord(marcRecord)
        .withMatchedId(secondSrsId)
        .withOrder(1)
        .withState(Record.State.DELETED)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId(secondInstanceId));

    async = testContext.async();
    List<Record> recordsToPost = Arrays.asList(record_1, record_2, record_3, record_4, record_6, deleted_record_1, deleted_record_2);
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

    List<String> ids = recordsToPost.stream()
      .filter(record -> Objects.nonNull(record.getParsedRecord()))
      .map(record -> record.getId())
      .collect(Collectors.toList());

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD&deleted=false")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(4))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD&deleted=true")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(5));
    async.complete();

    List<String> externalIds = recordsToPost.stream()
      .filter(record -> Objects.nonNull(record.getParsedRecord()))
      .map(record -> record.getExternalIdsHolder().getInstanceId())
      .collect(Collectors.toList());

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=INSTANCE&deleted=false")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(4))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=INSTANCE&deleted=true")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(5));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD")
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(4));
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
  public void shouldReturnParsedResultsWithAnyStateWithNoParametersSpecified(TestContext testContext) {
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

    Record recordWithOldState = new Record()
      .withId(SECOND_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    Record recordWithoutDeletedState = new Record()
      .withId(THIRD_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(0)
      .withState(Record.State.DELETED);

    Record recordWithActualState = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.ACTUAL);

    Record recordWithoutParsedRecord = new Record()
      .withId(FIRST_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL);

    List<Record> recordsToPost = Arrays.asList(recordWithOldState, recordWithoutDeletedState,
      recordWithActualState, recordWithoutParsedRecord);
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
      .body("totalRecords", is(1))
      .body("sourceRecords*.parsedRecord", notNullValue());
    async.complete();
  }

  @Test
  public void shouldReturnResultsOnGetBySpecifiedSnapshotId(TestContext testContext) {
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

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + record_2.getSnapshotId())
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
  public void shouldReturnAllSourceRecordsMarkedAsDeletedOnFindByRecordStateDeleted(TestContext testContext) {
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

    String matchedId = UUID.randomUUID().toString();

    Record record_3 = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
      .withOrder(11)
      .withState(Record.State.ACTUAL);

    async = testContext.async();
    createParsed = RestAssured.given()
      .spec(spec)
      .body(record_3)
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
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?deleted=true")
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
      .body(record_3)
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
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?orderBy=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
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

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
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
      // NOTE: we have to specify suppressFromDiscovery query parameter otherwise it will filter on the forced default of false
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + newRecord.getSnapshotId() + "&suppressFromDiscovery=true");
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    SourceRecordCollection sourceRecordCollection = getResponse.body().as(SourceRecordCollection.class);
    assertThat(sourceRecordCollection.getSourceRecords().size(), is(1));
    SourceRecord sourceRecord = sourceRecordCollection.getSourceRecords().get(0);
    assertThat(sourceRecord.getRecordId(), is(createdRecord.getId()));
    // NOTE: raw record is no longer returned with source records for effeciency
    // assertThat(sourceRecord.getRawRecord().getContent(), is(rawRecord.getContent()));
    assertThat(sourceRecord.getAdditionalInfo().getSuppressDiscovery(), is(createdRecord.getAdditionalInfo().getSuppressDiscovery()));
    async.complete();
  }

}
