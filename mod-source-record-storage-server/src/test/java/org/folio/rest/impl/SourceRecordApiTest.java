package org.folio.rest.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

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
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;

@RunWith(VertxUnitRunner.class)
public class SourceRecordApiTest extends AbstractRestVerticleTest {

  private static final String FIRST_UUID = UUID.randomUUID().toString();
  private static final String SECOND_UUID = UUID.randomUUID().toString();
  private static final String THIRD_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_UUID = UUID.randomUUID().toString();
  private static final String SIXTH_UUID = UUID.randomUUID().toString();
  private static final String SEVENTH_UUID = UUID.randomUUID().toString();
  private static final String EIGHTH_UUID = UUID.randomUUID().toString();
  private static final String NINTH_UUID = UUID.randomUUID().toString();

  private static final RawRecord rawRecord;
  private static final ParsedRecord marcRecord;
  private static final RawRecord rawEdifactRecord;
  private static final ParsedRecord parsedEdifactRecord;
  private static final ParsedRecord invalidParsedRecord;
  private static final ErrorRecord errorRecord;

  private static final Snapshot snapshot_1;
  private static final Snapshot snapshot_2;
  private static final Snapshot snapshot_3;
  private static final Snapshot snapshot_4;
  private static final Snapshot snapshot_5;

  private static final Record record_1;
  private static final Record record_2;
  private static final Record record_3;
  private static final Record record_4;
  private static final Record record_5;
  private static final Record record_6;
  private static final Record record_7;
  private static final Record record_8;
  private static final Record record_9;

  static {
    try {
      rawRecord = new RawRecord()
        .withContent(
          new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
      marcRecord = new ParsedRecord()
        .withContent(
          new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
            .encode());
      rawEdifactRecord = new RawRecord()
        .withContent(
          new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), String.class));
      parsedEdifactRecord = new ParsedRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH),
          JsonObject.class).encode());
      invalidParsedRecord = new ParsedRecord()
        .withContent(
          "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
      errorRecord = new ErrorRecord()
        .withDescription("Oops... something happened")
        .withContent(
          "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }

    snapshot_1 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    snapshot_2 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    snapshot_3 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    snapshot_4 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    snapshot_5 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    record_1 = new Record()
      .withId(FIRST_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL);
    record_2 = new Record()
      .withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_3 = new Record()
      .withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withErrorRecord(errorRecord)
      .withMatchedId(THIRD_UUID)
      .withState(Record.State.ACTUAL);
    record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_5 = new Record()
      .withId(FIFTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withMatchedId(FIFTH_UUID)
      .withParsedRecord(invalidParsedRecord)
      .withOrder(101)
      .withState(Record.State.ACTUAL);
    record_6 = new Record()
      .withId(SIXTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withMatchedId(SIXTH_UUID)
      .withParsedRecord(marcRecord)
      .withOrder(101)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_7 = new Record()
      .withId(SEVENTH_UUID)
      .withSnapshotId(snapshot_3.getJobExecutionId())
      .withRecordType(RecordType.EDIFACT)
      .withRawRecord(rawEdifactRecord)
      .withParsedRecord(parsedEdifactRecord)
      .withMatchedId(SEVENTH_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL);
    record_8 = new Record()
      .withId(EIGHTH_UUID)
      .withSnapshotId(snapshot_4.getJobExecutionId())
      .withRecordType(RecordType.MARC_AUTHORITY)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(EIGHTH_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_9 = new Record()
      .withId(NINTH_UUID)
      .withSnapshotId(snapshot_5.getJobExecutionId())
      .withRecordType(RecordType.MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(NINTH_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
  }

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
  public void shouldReturnSpecificMarcSourceRecordOnGetByRecordId(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    postRecords(testContext, record_1, record_3, record_7);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    Async async = testContext.async();
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
  public void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordId(TestContext testContext) {
    shouldReturnSpecificMarcRecordSourceRecordOnGetByRecordId(testContext, RecordType.MARC_AUTHORITY, record_8, snapshot_4);
  }

  @Test
  public void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByRecordId(TestContext testContext) {
    shouldReturnSpecificMarcRecordSourceRecordOnGetByRecordId(testContext, RecordType.MARC_HOLDING, record_9, snapshot_5);
  }

  @Test
  public void shouldReturnSpecificEdifactSourceRecordOnGetByRecordId(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    postRecords(testContext, record_1, record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_7)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(
        SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=EDIFACT&recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
    async.complete();
  }

  @Test
  public void shouldReturnSpecificMarcBibSourceRecordOnGetByDefaultExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByDefaultExternalId(testContext, snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  public void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByDefaultExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByDefaultExternalId(testContext, snapshot_5, RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByDefaultExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByDefaultExternalId(testContext, snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldReturnSpecificSourceRecordOnGetByInstanceExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByExternalId(testContext, snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  public void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByHoldingsExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByExternalId(testContext, snapshot_5, RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByHoldingsExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByExternalId(testContext, snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldReturnSpecificNumberOfMarcBibSourceRecordsOnGetByInstanceExternalHrid(TestContext testContext) {
    returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(testContext, snapshot_2, RecordType.MARC_BIB,
      "?instanceHrid=");
  }

  @Test
  public void shouldReturnSpecificNumberOfMarcHoldingsSourceRecordsOnGetByHoldingsExternalHrid(TestContext testContext) {
    returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(testContext, snapshot_5,
      RecordType.MARC_HOLDING, "?recordType=MARC_HOLDING&holdingsHrid=");
  }

  @Test
  public void shouldReturnSpecificNumberOfMarcHoldingsSourceRecordsOnGetByInstanceExternalHrid(TestContext testContext) {
    returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(testContext, snapshot_5,
      RecordType.MARC_HOLDING, "?recordType=MARC_HOLDING&externalHrid=");
  }

  @Test
  public void shouldReturnSpecificMarcBibSourceRecordOnGetByRecordExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByRecordExternalId(testContext, snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  public void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByRecordExternalId(testContext, snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldReturnSpecificMarcHoldingSourceRecordOnGetByRecordExternalId(TestContext testContext) {
    returnSpecificMarcSourceRecordOnGetByRecordExternalId(testContext, snapshot_5, RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldReturnSpecificMarcBibSourceRecordOnGetByRecordLeaderRecordStatus(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1, record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String leaderStatus = ParsedRecordDaoUtil.getLeaderStatus(createdRecord.getParsedRecord());

    Async async = testContext.async();
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
  public void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordLeaderRecordStatus(TestContext testContext) {
    shouldReturnSpecificMarcSourceRecordOnGetByRecordLeaderRecordStatus(testContext, RecordType.MARC_AUTHORITY, record_8,
      snapshot_4);
  }

  @Test
  public void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByRecordLeaderRecordStatus(TestContext testContext) {
    shouldReturnSpecificMarcSourceRecordOnGetByRecordLeaderRecordStatus(testContext, RecordType.MARC_HOLDING, record_9,
      snapshot_5);
  }

  @Test
  public void shouldReturnBadRequestOnGetIfInvalidExternalIdType(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Async async = testContext.async();
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
      .withRecordType(Record.RecordType.MARC_BIB)
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
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldNotReturnSpecificSourceRecordOnGetIfItIsNotExists(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Async async = testContext.async();
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
      .withRecordType(Record.RecordType.MARC_BIB)
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
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1, record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    Async async = testContext.async();
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
  public void shouldReturnEmptyCollectionOnGetByRecordIdIfThereIsNoSuchRecord(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1, record_2, record_3);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + UUID.randomUUID() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
    async.complete();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdAndRecordStateActualIfRecordWasDeleted(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    Async async = testContext.async();
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
  public void shouldReturnBadRequestOnGetByRecordIdIfInvalidUUID(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1, record_2);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId().substring(1).replace("-", "")
        + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldReturnSortedSourceRecordsOnGetWhenSortByIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    String firstMatchedId = UUID.randomUUID().toString();

    Record record_4_tmp = new Record()
      .withId(firstMatchedId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(firstMatchedId)
      .withOrder(1)
      .withState(Record.State.ACTUAL);

    String secondMathcedId = UUID.randomUUID().toString();

    Record record_2_tmp = new Record()
      .withId(secondMathcedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(secondMathcedId)
      .withOrder(11)
      .withState(Record.State.ACTUAL);

    postRecords(testContext, record_2, record_2_tmp, record_4, record_4_tmp, record_7);

    Async async = testContext.async();
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=MARC_BIB&orderBy=createdDate,DESC")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(4))
      .body("sourceRecords*.recordType", everyItem(is(RecordType.MARC_BIB.name())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertTrue(
      sourceRecordList.get(0).getMetadata().getCreatedDate().after(sourceRecordList.get(1).getMetadata().getCreatedDate()));
    testContext.assertTrue(
      sourceRecordList.get(1).getMetadata().getCreatedDate().after(sourceRecordList.get(2).getMetadata().getCreatedDate()));
    testContext.assertTrue(
      sourceRecordList.get(2).getMetadata().getCreatedDate().after(sourceRecordList.get(3).getMetadata().getCreatedDate()));
    async.complete();
  }

  @Test
  public void shouldReturnSortedMarcBibSourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_2, snapshot_3);

    postRecords(testContext, record_2, record_3, record_5, record_6, record_7);

    Async async = testContext.async();
    // NOTE: get source records will not return if there is no associated parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.recordType", everyItem(is(RecordType.MARC_BIB.name())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertEquals(11, sourceRecordList.get(0).getOrder());
    testContext.assertEquals(101, sourceRecordList.get(1).getOrder());
    async.complete();
  }

  @Test
  public void shouldReturnSortedMarcAuthoritySourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    shouldReturnSortedMarcSourceRecordsOnGetWhenSortByOrderIsSpecified(testContext, RecordType.MARC_AUTHORITY, record_8,
      snapshot_4);
  }

  @Test
  public void shouldReturnSortedMarcHoldingSourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    shouldReturnSortedMarcSourceRecordsOnGetWhenSortByOrderIsSpecified(testContext, RecordType.MARC_HOLDING, record_9,
      snapshot_5);
  }

  @Test
  public void shouldReturnSortedEdifactSourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_2, snapshot_3);

    postRecords(testContext, record_2, record_3, record_5, record_6, record_7);

    Async async = testContext.async();
    // NOTE: get source records will not return if there is no associated parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=EDIFACT&snapshotId=" + snapshot_3.getJobExecutionId()
        + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1))
      .body("sourceRecords*.recordType", everyItem(is(RecordType.EDIFACT.name())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertEquals(0, sourceRecordList.get(0).getOrder());
    async.complete();
  }

  @Test
  public void shouldReturnSourceRecordsForPeriod(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1);

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    Date fromDate = new Date();
    String from = dateTimeFormatter.format(ZonedDateTime.ofInstant(fromDate.toInstant(), ZoneId.systemDefault()));

    // NOTE: record_5 saves but fails parsed record content validation and does not save parsed record
    postRecords(testContext, record_2, record_3, record_4, record_5);

    Date toDate = new Date();
    String to = dateTimeFormatter.format(ZonedDateTime.ofInstant(toDate.toInstant(), ZoneId.systemDefault()));

    postRecords(testContext, record_6);

    Async async = testContext.async();
    // NOTE: we do not expect record_3 or record_5 as they do not have a parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + from + "&updatedBefore=" + to)
      .then()
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
      .then()
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
      .then()
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
      .then()
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
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
    async.complete();
  }

  @Test
  public void shouldReturnSourceRecordsByListOfId(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    String firstSrsId = UUID.randomUUID().toString();
    String firstInstanceId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord().withId(firstSrsId)
      .withContent(new JsonObject().put("leader", "01542dcm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", firstSrsId)).add(new JsonObject().put("i", firstInstanceId)))))));

    Record deleted_record_1 = new Record()
      .withId(firstSrsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(secondSrsId)
      .withOrder(1)
      .withState(Record.State.DELETED)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(secondInstanceId));

    Record[] records = new Record[] {record_1, record_2, record_3, record_4, record_6, deleted_record_1, deleted_record_2};
    postRecords(testContext, records);

    List<String> ids = Arrays.stream(records)
      .filter(record -> Objects.nonNull(record.getParsedRecord()))
      .map(Record::getId)
      .collect(Collectors.toList());

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD&deleted=false")
      .then()
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
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(5));
    async.complete();

    List<String> externalIds = Arrays.stream(records)
      .filter(record -> Objects.nonNull(record.getParsedRecord()))
      .map(record -> record.getExternalIdsHolder().getInstanceId())
      .collect(Collectors.toList());

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=INSTANCE&deleted=false")
      .then()
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
      .then()
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
      .then()
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
  public void shouldReturnMarcBibParsedResultsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(testContext, snapshot_3, record_7, 2, RecordType.MARC_BIB);
  }

  @Test
  public void shouldReturnMarcAuthorityParsedResultsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(testContext, snapshot_4, record_8, 1,
      RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldReturnMarcHoldingsParsedResultsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(testContext, snapshot_5, record_9, 1, RecordType.MARC_HOLDING);
  }

  @Test
  public void shouldReturnEdifactParsedResultsOnGetWhenReturnTypeQueryIsSpecified(TestContext testContext) {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(testContext, snapshot_3, record_7, 1, RecordType.EDIFACT);
  }

  @Test
  public void shouldReturnParsedResultsWithAnyStateWithNoParametersSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Record recordWithOldState = new Record()
      .withId(SECOND_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    Record recordWithoutDeletedState = new Record()
      .withId(THIRD_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(0)
      .withState(Record.State.DELETED);

    Record recordWithActualState = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.ACTUAL);

    Record recordWithoutParsedRecord = new Record()
      .withId(FIRST_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL);

    postRecords(testContext, recordWithOldState, recordWithoutDeletedState, recordWithActualState,
      recordWithoutParsedRecord);

    Async async = testContext.async();
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
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1, record_2, record_3, record_4);

    Async async = testContext.async();
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
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, record_1, record_2, record_3, record_4);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?limit=1")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", greaterThanOrEqualTo(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnAllSourceRecordsMarkedAsDeletedOnFindByRecordStateDeleted(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);

    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));

    Record parsedRecord = createParsed.body().as(Record.class);

    Async async = testContext.async();
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
      .withRecordType(Record.RecordType.MARC_BIB)
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
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", greaterThanOrEqualTo(2))
      .body("sourceRecords*.deleted", everyItem(is(true)));
    async.complete();
  }

  @Test
  public void shouldReturnOnlyUnmarkedAsDeletedSourceRecordOnGetWhenParameterDeletedIsNotPassed(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    postRecords(testContext, record_2);

    Async async = testContext.async();
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
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", greaterThanOrEqualTo(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnInvalidQueryParameters() {
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
    postSnapshots(testContext, snapshot_2);

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(true));

    Async async = testContext.async();
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
    assertThat(sourceRecord.getAdditionalInfo().getSuppressDiscovery(),
      is(createdRecord.getAdditionalInfo().getSuppressDiscovery()));
    async.complete();
  }

  private void shouldReturnSpecificMarcRecordSourceRecordOnGetByRecordId(TestContext testContext, RecordType recordType,
                                                                         Record record, Snapshot snapshot) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3, snapshot);

    postRecords(testContext, record_1, record_3, record_7);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=" + recordType + "&recordId=" + createdRecord.getId()
        + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
    async.complete();
  }

  private void returnSpecificMarcSourceRecordOnGetByDefaultExternalId(TestContext testContext, Snapshot snapshot_4,
                                                                      RecordType recordType) {
    postSnapshots(testContext, snapshot_1, snapshot_4);

    Async async = testContext.async();

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_4.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, null);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_4.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, null);

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
    var validatableResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + FIRST_UUID)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(FIRST_UUID));
    if (recordType == RecordType.MARC_BIB) {
      validatableResponse
        .body("externalIdsHolder.instanceId", is(SECOND_UUID));
    } else if (recordType == RecordType.MARC_HOLDING) {
      validatableResponse
        .body("externalIdsHolder.holdingsId", is(SECOND_UUID));
    } else if (recordType == RecordType.MARC_AUTHORITY) {
      validatableResponse
        .body("externalIdsHolder.authorityId", is(SECOND_UUID));
    }
    async.complete();
  }

  private void returnSpecificMarcSourceRecordOnGetByExternalId(TestContext testContext, Snapshot snapshot,
                                                               RecordType recordType) {
    postSnapshots(testContext, snapshot_1, snapshot);

    Async async = testContext.async();
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, null);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, null);

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

    String externalId = UUID.randomUUID().toString();

    Record recordWithOldState = new Record().withId(FOURTH_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD);
    setExternalIds(recordWithOldState, recordType, externalId, null);

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(record, recordType, externalId, null);

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
    var idType = getIdType(recordType);
    var validatableResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + externalId + "?idType=" + idType)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(THIRD_UUID));
    if (recordType == RecordType.MARC_BIB) {
      validatableResponse
        .body("externalIdsHolder.instanceId", is(externalId));
    } else if (recordType == RecordType.MARC_HOLDING) {
      validatableResponse
        .body("externalIdsHolder.holdingsId", is(externalId));
    } else if (recordType == RecordType.MARC_AUTHORITY) {
      validatableResponse
        .body("externalIdsHolder.authorityId", is(externalId));
    }
    async.complete();
  }

  private String getIdType(RecordType recordType){
    if (Record.RecordType.MARC_BIB == recordType) {
      return Record.RecordType.MARC_BIB.name();
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return Record.RecordType.MARC_HOLDING.name();
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      return Record.RecordType.MARC_AUTHORITY.name();
    } else {
      return null;
    }
  }

  private void returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(TestContext testContext,
                                                                          Snapshot snapshot, RecordType recordType,
                                                                          String url) {
    postSnapshots(testContext, snapshot_1, snapshot);

    Async async = testContext.async();

    String firstHrid = "123";
    String secondHrid = "1234";
    String thirdHrid = "1235";

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, firstHrid);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, secondHrid);

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
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD);
    setExternalIds(recordWithOldState, recordType, THIRD_UUID, thirdHrid);

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(record, recordType, SECOND_UUID, secondHrid);

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
    var validatableResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + url + secondHrid)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2));

    if (recordType == RecordType.MARC_BIB) {
      validatableResponse
        .body("sourceRecords*.externalIdsHolder.instanceHrid", everyItem(is(secondHrid)));
    } else if (recordType == RecordType.MARC_HOLDING) {
      validatableResponse
        .body("sourceRecords*.externalIdsHolder.holdingsHrid", everyItem(is(secondHrid)));
    }

    async.complete();
  }

  private void setExternalIds(Record record, RecordType recordType, String id, String hrid) {
    if (recordType == RecordType.MARC_BIB) {
      record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(id).withInstanceHrid(hrid));
    } else if (recordType == RecordType.MARC_HOLDING) {
      record.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(id).withHoldingsHrid(hrid));
    } else if (recordType == RecordType.MARC_AUTHORITY) {
      record.setExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(id));
    }
  }

  private void returnSpecificMarcSourceRecordOnGetByRecordExternalId(TestContext testContext, Snapshot snapshot,
                                                                     RecordType recordType) {
    postSnapshots(testContext, snapshot_1, snapshot);

    Async async = testContext.async();
    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, null);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, null);

    Record recordWithOldState = new Record().withId(FIFTH_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIFTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD);
    setExternalIds(recordWithOldState, recordType, FIRST_UUID, null);

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
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(record, recordType, instanceId, null);

    RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);
    async.complete();

    async = testContext.async();
    var validatableResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + SECOND_UUID + "?idType=RECORD")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(SECOND_UUID));
    if (recordType == RecordType.MARC_BIB) {
      validatableResponse
        .body("externalIdsHolder.instanceId", is(FIRST_UUID));
    } else if (recordType == RecordType.MARC_HOLDING) {
      validatableResponse
        .body("externalIdsHolder.holdingsId", is(FIRST_UUID));
    } else if (recordType == RecordType.MARC_AUTHORITY) {
      validatableResponse
        .body("externalIdsHolder.instanceId", nullValue())
        .body("externalIdsHolder.holdingsId", nullValue());

    }
    async.complete();
  }

  private void shouldReturnSpecificMarcSourceRecordOnGetByRecordLeaderRecordStatus(TestContext testContext,
                                                                                   RecordType recordType, Record record,
                                                                                   Snapshot snapshot) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot);

    postRecords(testContext, record_1, record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String leaderStatus = ParsedRecordDaoUtil.getLeaderStatus(createdRecord.getParsedRecord());

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=" + recordType + "&leaderRecordStatus=" + leaderStatus
        + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
    async.complete();
  }

  private void shouldReturnSortedMarcSourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext,
                                                                                  RecordType recordType, Record record,
                                                                                  Snapshot snapshot) {
    postSnapshots(testContext, snapshot_2, snapshot_3, snapshot);

    postRecords(testContext, record_2, record_3, record_5, record_6, record_7, record);

    Async async = testContext.async();
    // NOTE: get source records will not return if there is no associated parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=" + recordType + "&snapshotId=" + snapshot.getJobExecutionId()
        + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1))
      .body("sourceRecords*.recordType", everyItem(is(recordType.name())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    testContext.assertEquals(0, sourceRecordList.get(0).getOrder());
    async.complete();
  }

  private void shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(TestContext testContext, Snapshot snapshot,
                                                                        Record record, int totalRecords,
                                                                        RecordType recordType) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot);

    postRecords(testContext, record_1, record_2, record_3, record_4, record);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=" + recordType)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(totalRecords))
      .body("sourceRecords*.recordType", everyItem(is(recordType.name())))
      .body("sourceRecords*.parsedRecord", notNullValue())
      .body("sourceRecords*.deleted", everyItem(is(false)));
    async.complete();
  }

}
