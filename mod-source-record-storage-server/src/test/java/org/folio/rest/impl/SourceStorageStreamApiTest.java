package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class SourceStorageStreamApiTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_STREAM_RECORDS_PATH = "/source-storage/stream/records";
  private static final String SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH = "/source-storage/stream/source-records";

  private static final String FIRST_UUID = UUID.randomUUID().toString();
  private static final String SECOND_UUID = UUID.randomUUID().toString();
  private static final String THIRD_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_UUID = UUID.randomUUID().toString();
  private static final String SIXTH_UUID = UUID.randomUUID().toString();
  private static final String SEVENTH_UUID = UUID.randomUUID().toString();
  private static final String EIGHTH_UUID = UUID.randomUUID().toString();
  private static final String FIRST_HRID = "id1234567";

  private static RawRecord rawRecord;
  private static ParsedRecord marcRecord;
  private static ParsedRecord marcRecordWith001;

  static {
    try {
      rawRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
      marcRecord = new ParsedRecord()
        .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
      marcRecordWith001 = new ParsedRecord()
        .withContent(new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("001", FIRST_HRID))).encode());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

    private static final ErrorRecord errorRecord = new ErrorRecord()
    .withDescription("Oops... something happened")
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");

    private static final Snapshot snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    private static final Snapshot snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    private static final Snapshot snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    private static final Record marc_bib_record_1 = new Record()
    .withId(FIRST_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecordWith001)
    .withMatchedId(FIRST_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
    .withInstanceId(FIFTH_UUID)
    .withInstanceHrid(FIRST_HRID));
    private static final Record marc_bib_record_2 = new Record()
    .withId(SECOND_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(SECOND_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
    private static final Record marc_bib_record_3 = new Record()
    .withId(THIRD_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecordWith001)
    .withErrorRecord(errorRecord)
    .withOrder(101)
    .withMatchedId(THIRD_UUID)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(FIFTH_UUID)
      .withInstanceHrid(FIRST_HRID));
    private static final Record marc_bib_record_4 = new Record()
    .withId(FOURTH_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(FOURTH_UUID)
    .withOrder(1)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
    private static final Record marc_bib_record_5 = new Record()
    .withId(SIXTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withMatchedId(SIXTH_UUID)
    .withParsedRecord(marcRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
    private static final Record marc_auth_record_1 = new Record()
    .withId(SEVENTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(RecordType.MARC_AUTHORITY)
    .withRawRecord(rawRecord)
    .withMatchedId(SEVENTH_UUID)
    .withParsedRecord(marcRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withAuthorityId(UUID.randomUUID().toString())
      .withAuthorityHrid("12345"));
    private static final Record marc_holdings_record_1 = new Record()
    .withId(EIGHTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(RecordType.MARC_HOLDING)
    .withRawRecord(rawRecord)
    .withMatchedId(EIGHTH_UUID)
    .withParsedRecord(marcRecord)
    .withOrder(101)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withHoldingsId(UUID.randomUUID().toString())
      .withHoldingsHrid("12345"));

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
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnAllRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Record record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2, marc_bib_record_3, record_4);

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
        testContext.assertEquals(4, actual.size());
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnAllMarcAuthorityRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnAllMarcRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(testContext, RecordType.MARC_AUTHORITY, marc_auth_record_1);
  }

  @Test
  public void shouldReturnAllMarcHoldingsRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnAllMarcRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(testContext, RecordType.MARC_HOLDING, marc_holdings_record_1);
  }

  private void shouldReturnAllMarcRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext, RecordType recordType, Record marc_auth_record_1) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Record record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_3.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID))
      .withState(Record.State.OLD);

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2, marc_bib_record_3, record_4, marc_auth_record_1);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH + "?recordType=" + recordType)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(2, actual.size());
        async.complete();
      }).collect(() -> actual, List::add)
      .subscribe();
  }

  @Test
  public void shouldReturnRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2, marc_bib_record_3, recordWithOldStatus);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH + "?state=ACTUAL&snapshotId=" + marc_bib_record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

      List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(2, actual.size());
        testContext.assertEquals(marc_bib_record_2.getSnapshotId(), actual.get(0).getSnapshotId());
        testContext.assertEquals(marc_bib_record_2.getSnapshotId(), actual.get(1).getSnapshotId());
        testContext.assertEquals(false, actual.get(1).getAdditionalInfo().getSuppressDiscovery());
        testContext.assertEquals(false, actual.get(1).getAdditionalInfo().getSuppressDiscovery());
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

 @Test
  public void shouldReturnMarcAuthorityRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
   shouldReturnMarcRecordsOnGetBySpecifiedSnapshotId(testContext, RecordType.MARC_AUTHORITY, marc_auth_record_1);
 }

 @Test
  public void shouldReturnMarcHoldingsRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
   shouldReturnMarcRecordsOnGetBySpecifiedSnapshotId(testContext, RecordType.MARC_HOLDING, marc_holdings_record_1);
 }

  private void shouldReturnMarcRecordsOnGetBySpecifiedSnapshotId(TestContext testContext, RecordType marcHolding, Record marc_holdings_record_1) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_3.getJobExecutionId())
      .withRecordType(marcHolding)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2, marc_bib_record_3, marc_holdings_record_1, recordWithOldStatus);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_RECORDS_PATH + "?recordType=" + marcHolding + "&state=ACTUAL&snapshotId=" + marc_holdings_record_1.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<Record> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, Record.class))
      .doFinally(() -> {
        testContext.assertEquals(1, actual.size());
        testContext.assertEquals(marc_holdings_record_1.getSnapshotId(), actual.getFirst().getSnapshotId());
        testContext.assertEquals(false, actual.getFirst().getAdditionalInfo().getSuppressDiscovery());
        async.complete();
      }).collect(() -> actual, List::add)
      .subscribe();
  }

  @Test
  public void shouldReturnLimitedCollectionWithActualStateOnGetWithLimit(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2, marc_bib_record_3, recordWithOldStatus);

    final Async async = testContext.async();
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
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnSpecificNumberOfSourceRecordsOnGetByInstanceExternalHrid(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Async async = testContext.async();

    String firstHrid = "123";
    String secondHrid = "1234";
    String thirdHrid = "1235";

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(firstHrid));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(THIRD_UUID).withInstanceHrid(thirdHrid));

    Record record = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FOURTH_UUID).withInstanceHrid(secondHrid));

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

    final Async finalAsync = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?instanceHrid=" + secondHrid)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(2, actual.size());
        testContext.assertTrue(Objects.nonNull(actual.get(0).getParsedRecord()));
        testContext.assertTrue(Objects.nonNull(actual.get(1).getParsedRecord()));
        testContext.assertEquals(secondHrid, actual.get(0).getExternalIdsHolder().getInstanceHrid());
        testContext.assertEquals(secondHrid, actual.get(1).getExternalIdsHolder().getInstanceHrid());
        finalAsync.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnSpecificSourceRecordOnGetByRecordLeaderRecordStatus(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, marc_bib_record_1, marc_bib_record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(marc_bib_record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String leaderStatus = ParsedRecordDaoUtil.getLeaderStatus(createdRecord.getParsedRecord());

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?leaderRecordStatus=" + leaderStatus + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(1, actual.size());
        testContext.assertTrue(Objects.nonNull(actual.getFirst().getParsedRecord()));
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdIfParsedRecordIsNull(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, marc_bib_record_1, marc_bib_record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(marc_bib_record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(0, actual.size());
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdIfThereIsNoSuchRecord(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2, marc_bib_record_3);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
            .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?recordId=" + UUID.randomUUID() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(0, actual.size());
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnEmptyCollectionOnGetByRecordIdAndRecordStateActualIfRecordWasDeleted(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(marc_bib_record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);


    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    final Async finalAsync = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?recordId=" + parsed.getId() + "&recordState=ACTUAL&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(0, actual.size());
        finalAsync.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnErrorOnGetByRecordIdIfInvalidUUID(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, marc_bib_record_1, marc_bib_record_2);

    Record createdRecord =
      RestAssured.given()
        .spec(spec)
        .body(marc_bib_record_3)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .body().as(Record.class);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId().substring(1).replace("-", "") + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldReturnSortedSourceRecordsOnGetWhenSortByIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    String firstMatchedId = UUID.randomUUID().toString();

    Record record_4_tmp = new Record()
      .withId(firstMatchedId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(firstMatchedId)
      .withOrder(1)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(SECOND_UUID)
        .withInstanceHrid(FIRST_HRID));

    String secondMathcedId = UUID.randomUUID().toString();

    Record record_2_tmp = new Record()
      .withId(secondMathcedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(secondMathcedId)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, marc_bib_record_2, record_2_tmp, marc_bib_record_4, record_4_tmp);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?recordType=MARC_BIB&orderBy=createdDate,DESC")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(4, actual.size());
        testContext.assertTrue(Objects.nonNull(actual.get(0).getParsedRecord()));
        testContext.assertTrue(Objects.nonNull(actual.get(1).getParsedRecord()));
        testContext.assertTrue(Objects.nonNull(actual.get(2).getParsedRecord()));
        testContext.assertTrue(Objects.nonNull(actual.get(3).getParsedRecord()));
        testContext.assertEquals(false, actual.get(0).getDeleted());
        testContext.assertEquals(false, actual.get(1).getDeleted());
        testContext.assertEquals(false, actual.get(2).getDeleted());
        testContext.assertEquals(false, actual.get(3).getDeleted());
        testContext.assertTrue(actual.get(0).getMetadata().getCreatedDate().after(actual.get(1).getMetadata().getCreatedDate()));
        testContext.assertTrue(actual.get(1).getMetadata().getCreatedDate().after(actual.get(2).getMetadata().getCreatedDate()));
        testContext.assertTrue(actual.get(2).getMetadata().getCreatedDate().after(actual.get(3).getMetadata().getCreatedDate()));
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnSortedSourceRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    postRecords(testContext, marc_bib_record_2, marc_bib_record_3);

    final Async async = testContext.async();
    InputStream response = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?snapshotId=" + snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> actual = new ArrayList<>();
    flowableInputStreamScanner(response)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertEquals(2, actual.size());
        testContext.assertTrue(Objects.nonNull(actual.get(0).getParsedRecord()));
        testContext.assertTrue(Objects.nonNull(actual.get(1).getParsedRecord()));
        testContext.assertEquals(false, actual.get(0).getDeleted());
        testContext.assertEquals(false, actual.get(1).getDeleted());
        testContext.assertEquals(11, actual.get(0).getOrder());
        testContext.assertEquals(101, actual.get(1).getOrder());
        async.complete();
      }).collect(() -> actual, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnSourceRecordsForPeriod(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    postRecords(testContext, marc_bib_record_1);

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    Date fromDate = new Date();
    String from = dateTimeFormatter.format(ZonedDateTime.ofInstant(fromDate.toInstant(), ZoneId.systemDefault()));

    postRecords(testContext, marc_bib_record_2, marc_bib_record_3, marc_bib_record_4);

    Date toDate = new Date();
    String to = dateTimeFormatter.format(ZonedDateTime.ofInstant(toDate.toInstant(), ZoneId.systemDefault()));

    Async async = testContext.async();
    RestAssured.given()
        .spec(spec)
        .body(marc_bib_record_5)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    final Async finalAsync = testContext.async();
    // NOTE: we do not marc_holdings_record_2 as they do not have a parsed record
    InputStream result = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?updatedAfter=" + from + "&updatedBefore=" + to)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().asInputStream();

    List<SourceRecord> sourceRecordList = new ArrayList<>();
    flowableInputStreamScanner(result)
      .map(r -> Json.decodeValue(r, SourceRecord.class))
      .doFinally(() -> {
        testContext.assertTrue(sourceRecordList.get(0).getMetadata().getUpdatedDate().after(fromDate));
        testContext.assertTrue(sourceRecordList.get(1).getMetadata().getUpdatedDate().after(fromDate));
        testContext.assertTrue(sourceRecordList.get(0).getMetadata().getUpdatedDate().before(toDate));
        testContext.assertTrue(sourceRecordList.get(1).getMetadata().getUpdatedDate().before(toDate));

        Async innerAsync = testContext.async();
        RestAssured.given()
          .spec(spec)
          .when()
          .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + from)
          .then()
          .statusCode(HttpStatus.SC_OK)
          .body("sourceRecords.size()", is(4))
          .body("totalRecords", is(4))
          .body("sourceRecords*.deleted", everyItem(is(false)));
        innerAsync.complete();

        innerAsync = testContext.async();
        RestAssured.given()
          .spec(spec)
          .when()
          .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + to)
          .then()
          .statusCode(HttpStatus.SC_OK)
          .body("sourceRecords.size()", is(1))
          .body("totalRecords", is(1))
          .body("sourceRecords*.deleted", everyItem(is(false)));
        innerAsync.complete();

        innerAsync = testContext.async();
        RestAssured.given()
          .spec(spec)
          .when()
          .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedBefore=" + to)
          .then()
          .statusCode(HttpStatus.SC_OK)
          .body("sourceRecords.size()", is(3))
          .body("totalRecords", is(3))
          .body("sourceRecords*.deleted", everyItem(is(false)));
        innerAsync.complete();

        InputStream response = RestAssured.given()
          .spec(spec)
          .when()
          .get(SOURCE_STORAGE_STREAM_SOURCE_RECORDS_PATH + "?updatedBefore=" + from)
          .then()
          .statusCode(HttpStatus.SC_OK)
          .extract().response().asInputStream();

        List<SourceRecord> actual = new ArrayList<>();
        flowableInputStreamScanner(response)
          .map(r -> Json.decodeValue(r, SourceRecord.class))
          .doFinally(() -> {
            testContext.assertEquals(0, actual.size());
            finalAsync.complete();
          }).collect(() -> actual, List::add)
            .subscribe();
      }).collect(() -> sourceRecordList, List::add)
        .subscribe();
  }

  @Test
  public void shouldReturnBadRequestOnSearchMarcRecordIdsWhenExpressionsAreMissing(TestContext testContext) {
    // given
    final Async async = testContext.async();
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression(null);
    searchRequest.setFieldsSearchExpression(null);
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    // then
    assertEquals(HttpStatus.SC_BAD_REQUEST, response.statusCode());
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnSearchMarcRecordIdsWhenFieldsSearchExpressionIsWrong(TestContext testContext) {
    // given
    final Async async = testContext.async();
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '3451991' and 005.value = '20140701')");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    // then
    assertEquals(HttpStatus.SC_BAD_REQUEST, response.statusCode());
    async.complete();
  }

  @Test
  public void shouldReturnEmptyResponseOnSearchMarcRecordIdsWhenNoRecordsPosted(TestContext testContext) {
    // given
    final Async async = testContext.async();
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '3451991'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(0, responseBody.getJsonArray("records").size());
    assertEquals(0, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdOnSearchMarcRecordIdsWhenSearchByFieldsSearchExpression(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression(
      "001.value = '393893' " +
      "and 005.value ^= '2014110' " +
      "and 035.ind1 = '#' " +
      "and 005.03_02 = '41' " +
      "and 005.date in '20120101-20190101' " +
      "and 035.value is 'present' " +
      "and 999.value is 'present' " +
      "and 041.g is 'present' " +
      "and 041.z is 'absent' " +
      "and 050.ind1 is 'present' " +
      "and 050.ind2 is 'absent'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdOnSearchMarcRecordIdsWhenSearchByLeaderSearchExpression(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdOnSearchMarcRecordIdsWhenSearchByLeaderSearchExpressionAndFieldsSearchExpression(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnEmptyResponseOnSearchMarcRecordIdsWhenRecordWasDeleted(TestContext testContext) {
    // given
    postSnapshots(testContext, snapshot_2);
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(marc_bib_record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    // when
    async = testContext.async();
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(0, responseBody.getJsonArray("records").size());
    assertEquals(0, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdOnSearchMarcRecordIdsWhenRecordWasDeleted(TestContext testContext) {
    // given
    postSnapshots(testContext, snapshot_2);
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(marc_bib_record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'd' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setSuppressFromDiscovery(true);
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    searchRequest.setDeleted(true);
    // when
    async = testContext.async();
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnEmptyResponseOnSearchMarcRecordIdsWhenRecordWasSuppressed(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID))
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true));
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    searchRequest.setSuppressFromDiscovery(false);
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(0, responseBody.getJsonArray("records").size());
    assertEquals(0, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnRecordOnSearchMarcRecordWhenRecordWasSuppressedAndSetSuppressFromDiscoveryNotSetInRequest(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true))
      .withExternalIdsHolder(marc_bib_record_2.getExternalIdsHolder());
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnRecordOnSearchMarcRecordWhenRecordWasNotSuppressedAndSetSuppressFromDiscoveryNotSetInRequest(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false))
      .withExternalIdsHolder(marc_bib_record_2.getExternalIdsHolder());
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }


  @Test
  public void shouldReturnRecordOnSearchMarcRecordWhenRecordWasDeletedAndDeletedNotSetInRequest(TestContext testContext) {
    // given
    postSnapshots(testContext, snapshot_2);
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(marc_bib_record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'd'");
    // when
    async = testContext.async();
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnEmptyResponseOnSearchMarcRecordIdsWhenLimitIs0(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    searchRequest.setLimit(0);
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(0, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdOnSearchMarcRecordIdsWhenLimitIs1(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '393893' and 005.value ^= '2014110' and 035.ind1 = '#'");
    searchRequest.setLimit(1);
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnEmptyResponseOnSearchMarcRecordIdsWhenOffsetIs1(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '393893'");
    searchRequest.setOffset(1);
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(0, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdResponseOnSearchMarcRecordIdsWhenMarcBibAndAuthoritySaved(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_bib_record_2, marc_auth_record_1);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '393893'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnEmptyResponseOnSearchMarcRecordIdsWhenMarcBibAndAuthoritySaved(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, marc_auth_record_1);
    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '393893'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(0, responseBody.getJsonArray("records").size());
    assertEquals(0, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnIdOnSearchMarcRecordIdsWhenInstanceIdIsMissing(TestContext testContext) {
    // given
    final Async async = testContext.async();
    postSnapshots(testContext, snapshot_2);
    Record marc_bib_record_withoutInstanceId = new Record()
      .withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FIFTH_UUID)
        .withInstanceHrid(FIRST_HRID));
    postRecords(testContext, marc_bib_record_2, marc_bib_record_withoutInstanceId);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setFieldsSearchExpression("001.value = '393893'");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

    @Test
    public void shouldProcessSearchQueryIfSearchNeededWithinOneField(TestContext testContext) {
        // given
        final Async async = testContext.async();
        postSnapshots(testContext, snapshot_2);
        postRecords(testContext, marc_bib_record_2);
        MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
        searchRequest.setFieldsSearchExpression("050.a ^= 'M3' and 050.b ^= '.M896'");
        // when
        ExtractableResponse<Response> response = RestAssured.given()
                .spec(spec)
                .body(searchRequest)
                .when()
                .post("/source-storage/stream/marc-record-identifiers")
                .then()
                .extract();
        JsonObject responseBody = new JsonObject(response.body().asString());
        // then
        assertEquals(HttpStatus.SC_OK, response.statusCode());
        assertEquals(1, responseBody.getJsonArray("records").size());
        assertEquals(1, responseBody.getInteger("totalCount").intValue());
        async.complete();
    }

    @Test
    public void shouldProcessSearchQueryIfSearchNeededWithinOneFieldWithParenthesis(TestContext testContext) {
        // given
        final Async async = testContext.async();
        postSnapshots(testContext, snapshot_2);
        postRecords(testContext, marc_bib_record_2);
        MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
        searchRequest.setFieldsSearchExpression("(050.a ^= 'M3' and 050.b ^= '.M896') and 240.a ^= 'Works'");
        // when
        ExtractableResponse<Response> response = RestAssured.given()
                .spec(spec)
                .body(searchRequest)
                .when()
                .post("/source-storage/stream/marc-record-identifiers")
                .then()
                .extract();
        JsonObject responseBody = new JsonObject(response.body().asString());
        // then
        assertEquals(HttpStatus.SC_OK, response.statusCode());
        assertEquals(1, responseBody.getJsonArray("records").size());
        assertEquals(1, responseBody.getInteger("totalCount").intValue());
        async.complete();
    }

  @Test
  public void shouldReturn400WithIncorrectRequest(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true))
      .withExternalIdsHolder(marc_bib_record_2.getExternalIdsHolder());
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("(035.a = '(OCoLC)63611770' and 036.ind1 = '1' or (245.a ^= 'Semantic web' and 005.value ^= '20141107')");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    // then
    assertEquals(HttpStatus.SC_BAD_REQUEST, response.statusCode());
    assertEquals("The number of opened brackets should be equal to number of closed brackets [expression: marcFieldSearchExpression]",
      response.body().asString());
    async.complete();
  }

  @Test
  public void shouldReturnDataForDocumentationExample(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true))
      .withExternalIdsHolder(marc_bib_record_2.getExternalIdsHolder());
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("(035.a = '(OCoLC)63611770' and 036.ind1 = '1') or (245.a ^= 'Neue Ausgabe sämtlicher' and 005.value ^= '20141107')");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnDataForNotEqualsOperator(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true))
      .withExternalIdsHolder(marc_bib_record_2.getExternalIdsHolder());
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("(035.a = '(OCoLC)63611770' and 948.ind1 not= '5')");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  @Test
  public void shouldReturnDataForOneFieldNoOperator(TestContext testContext) {
    // given
    Async async = testContext.async();
    Record suppressedRecord = new Record()
      .withId(marc_bib_record_2.getId())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(marc_bib_record_2.getRawRecord())
      .withParsedRecord(marc_bib_record_2.getParsedRecord())
      .withMatchedId(marc_bib_record_2.getMatchedId())
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(true))
      .withExternalIdsHolder(marc_bib_record_2.getExternalIdsHolder());
    postSnapshots(testContext, snapshot_2);
    postRecords(testContext, suppressedRecord);

    MarcRecordSearchRequest searchRequest = new MarcRecordSearchRequest();
    searchRequest.setLeaderSearchExpression("p_05 = 'c' and p_06 = 'c' and p_07 = 'm'");
    searchRequest.setFieldsSearchExpression("(948.ind1 = '2' and 948.value = '20130128')" +
      "  and (948.ind1 = '2' and 948.value = '20141106') " +
      "  and (948.ind1 = '2' and 948.value = 'm') " +
      "  and (948.ind1 = '2' and 948.value = 'batch')");
    // when
    ExtractableResponse<Response> response = RestAssured.given()
      .spec(spec)
      .body(searchRequest)
      .when()
      .post("/source-storage/stream/marc-record-identifiers")
      .then()
      .extract();
    JsonObject responseBody = new JsonObject(response.body().asString());
    // then
    assertEquals(HttpStatus.SC_OK, response.statusCode());
    assertEquals(1, responseBody.getJsonArray("records").size());
    assertEquals(1, responseBody.getInteger("totalCount").intValue());
    async.complete();
  }

  private Flowable<String> flowableInputStreamScanner(InputStream inputStream) {
    return Flowable.create(subscriber -> {
        try (Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8)) {
        while (scanner.hasNext()) {
          subscriber.onNext(scanner.nextLine());
        }
      }
      subscriber.onComplete();
    }, BackpressureStrategy.BUFFER);
  }

}
