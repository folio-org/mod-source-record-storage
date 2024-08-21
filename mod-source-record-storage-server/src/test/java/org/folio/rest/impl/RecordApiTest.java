package org.folio.rest.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;

@RunWith(VertxUnitRunner.class)
public class RecordApiTest extends AbstractRestVerticleTest {

  private static final String FIRST_UUID = UUID.randomUUID().toString();
  private static final String SECOND_UUID = UUID.randomUUID().toString();
  private static final String THIRD_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_UUID = UUID.randomUUID().toString();
  private static final String SIXTH_UUID = UUID.randomUUID().toString();
  private static final String SEVENTH_UUID = UUID.randomUUID().toString();
  private static final String EIGHTH_UUID = UUID.randomUUID().toString();
  private static final String FIRST_HRID = RandomStringUtils.randomAlphanumeric(9);
  private static final String GENERATION = "generation";

  private static RawRecord rawMarcRecord;
  private static ParsedRecord parsedMarcRecord;
  private static ParsedRecord marcRecordWith001;

  private static RawRecord rawEdifactRecord;
  private static ParsedRecord parsedEdifactRecord;
  private static ParsedRecord parsedMarcRecordWith001and999ff$s;

  static {
    try {
      rawMarcRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
      parsedMarcRecord = new ParsedRecord()
        .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
      marcRecordWith001 = new ParsedRecord()
        .withContent(new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("001", FIRST_HRID))).encode());
      rawEdifactRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), String.class));
      parsedEdifactRecord = new ParsedRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
      parsedMarcRecordWith001and999ff$s = new ParsedRecord().withId(FIRST_UUID)
        .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
          .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
            .put("subfields",
              new JsonArray().add(new JsonObject().put("s", FIRST_UUID)))
            .put("ind1", "f")
            .put("ind2", "f")))
            .add(new JsonObject().put("001", FIRST_HRID))).encode());
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
  private static Snapshot snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot snapshot_4 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

  private static Record record_1 = new Record()
    .withId(FIRST_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(marcRecordWith001)
    .withMatchedId(FIRST_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
  private static Record record_2 = new Record()
    .withId(SECOND_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(parsedMarcRecord)
    .withMatchedId(SECOND_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
  private static Record record_3 = new Record()
    .withId(THIRD_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawMarcRecord)
    .withErrorRecord(errorRecord)
    .withParsedRecord(marcRecordWith001)
    .withMatchedId(THIRD_UUID)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
  private static Record record_5 = new Record()
    .withId(FIFTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(FIFTH_UUID)
    .withParsedRecord(marcRecordWith001)
    .withOrder(101)
    .withState(Record.State.ACTUAL)
    .withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(UUID.randomUUID().toString())
      .withInstanceHrid(FIRST_HRID));
  private static Record record_6 = new Record()
    .withId(SIXTH_UUID)
    .withSnapshotId(snapshot_3.getJobExecutionId())
    .withRecordType(Record.RecordType.EDIFACT)
    .withRawRecord(rawEdifactRecord)
    .withParsedRecord(parsedEdifactRecord)
    .withMatchedId(SIXTH_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record record_7 = new Record()
    .withId(SEVENTH_UUID)
    .withSnapshotId(snapshot_4.getJobExecutionId())
    .withRecordType(RecordType.MARC_AUTHORITY)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(parsedMarcRecord)
    .withMatchedId(SEVENTH_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record record_8 = new Record()
    .withId(SEVENTH_UUID)
    .withSnapshotId(snapshot_4.getJobExecutionId())
    .withRecordType(RecordType.MARC_HOLDING)
    .withRawRecord(rawMarcRecord)
    .withParsedRecord(parsedMarcRecord)
    .withMatchedId(EIGHTH_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record record_9 = new Record()
    .withId(FIFTH_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(RecordType.MARC_AUTHORITY)
    .withRawRecord(rawMarcRecord)
    .withMatchedId(FIFTH_UUID)
    .withParsedRecord(invalidParsedRecord)
    .withOrder(101)
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
  public void shouldReturnAllMarcBibRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Record record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid(FIRST_HRID));;

    postRecords(testContext, record_1, record_2, record_3, record_4, record_6);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(4))
      .body("records*.recordType", everyItem(is(RecordType.MARC_BIB.name())))
      .body("records*.state", everyItem(notNullValue()));
    async.complete();
  }

  @Test
  public void shouldReturnAllMarcAuthorityRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnAllMarcRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(testContext, RecordType.MARC_AUTHORITY, record_7);
  }

  @Test
  public void shouldReturnAllMarcHoldingsRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    shouldReturnAllMarcRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(testContext, RecordType.MARC_HOLDING, record_8);
  }

  public void shouldReturnAllMarcRecordsWithNotEmptyStateOnGetWhenNoQueryIsSpecified(TestContext testContext,
                                                                              RecordType recordType, Record record) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3, snapshot_4);

    Record record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_4.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    postRecords(testContext, record_1, record_2, record_3, record_4, record);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=" + recordType)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("records*.recordType", everyItem(is(recordType.name())))
      .body("records*.state", everyItem(notNullValue()));
    async.complete();
  }

  @Test
  public void shouldReturnAllEdifactRecordsWithNotEmptyStateOnGetWhenRecordTypeQueryIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Record record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FOURTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, record_1, record_2, record_3, record_4, record_6);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=EDIFACT")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("records*.recordType", everyItem(is(RecordType.EDIFACT.name())))
      .body("records*.state", everyItem(notNullValue()));
    async.complete();
  }

  @Test
  public void shouldReturnMarcBibRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FOURTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, record_1, record_2, record_3, record_6, recordWithOldStatus);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=MARC_BIB&state=ACTUAL&snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("records*.recordType", everyItem(is(RecordType.MARC_BIB.name())))
      .body("records*.snapshotId", everyItem(is(record_2.getSnapshotId())))
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnMarcAuthorityRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    shouldReturnMarcRecordsOnGetBySpecifiedSnapshotId(testContext, RecordType.MARC_AUTHORITY, record_7);
  }

  @Test
  public void shouldReturnMarcHoldingRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    shouldReturnMarcRecordsOnGetBySpecifiedSnapshotId(testContext, RecordType.MARC_HOLDING, record_8);
  }

  public void shouldReturnMarcRecordsOnGetBySpecifiedSnapshotId(TestContext testContext, RecordType recordType,
                                                                Record record){
    postSnapshots(testContext,  snapshot_1, snapshot_2, snapshot_3, snapshot_4);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD);

    postRecords(testContext, record_1, record_2, record_3, record, recordWithOldStatus);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=" + recordType + "&state=ACTUAL&snapshotId=" + record.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("records*.recordType", everyItem(is(recordType.name())))
      .body("records*.snapshotId", everyItem(is(record.getSnapshotId())))
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnEdifactRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FOURTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, record_1, record_2, record_3, record_6, recordWithOldStatus);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=EDIFACT&state=ACTUAL&snapshotId=" + record_6.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("records*.recordType", everyItem(is(RecordType.EDIFACT.name())))
      .body("records*.snapshotId", everyItem(is(record_6.getSnapshotId())))
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)));
    async.complete();
  }

  @Test
  public void shouldReturnLimitedCollectionWithActualStateOnGetWithLimit(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2);

    Record recordWithOldStatus = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(FOURTH_UUID)
        .withInstanceHrid(FIRST_HRID));

    postRecords(testContext, record_1, record_2, record_3, recordWithOldStatus);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?state=ACTUAL&limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(2))
      .body("totalRecords", is(3));
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
  public void shouldCreateMarcRecordOnPost(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_1.getSnapshotId()))
      .body("recordType", is(record_1.getRecordType().name()))
      .body("rawRecord.content", is(rawMarcRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldCreateEdifactRecordOnPost(TestContext testContext) {
    postSnapshots(testContext, snapshot_3);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_6)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_6.getSnapshotId()))
      .body("recordType", is(record_6.getRecordType().name()))
      .body("rawRecord.content", is(rawEdifactRecord.getContent()))
      .body("additionalInfo.suppressDiscovery", is(false));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordOnPost(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_3.getSnapshotId()))
      .body("recordType", is(record_3.getRecordType().name()))
      .body("rawRecord.content", is(rawMarcRecord.getContent()))
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
  public void shouldUpdateExistingMarcRecordOnPut(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response putResponse = RestAssured.given()
      .spec(spec)
      .body(createdRecord.withParsedRecord(parsedMarcRecord))
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(putResponse.statusCode(), is(HttpStatus.SC_OK));
    Record updatedRecord = putResponse.body().as(Record.class);
    assertThat(updatedRecord.getId(), is(createdRecord.getId()));
    assertThat(updatedRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    assertThat(updatedRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldUpdateExistingEdifactRecordOnPut(TestContext testContext) {
    postSnapshots(testContext, snapshot_3);

    String id = UUID.randomUUID().toString();

    Record recordWithoutParsedRecord = new Record()
      .withId(id)
      .withSnapshotId(snapshot_3.getJobExecutionId())
      .withRecordType(Record.RecordType.EDIFACT)
      .withRawRecord(rawEdifactRecord)
      .withMatchedId(id)
      .withOrder(0)
      .withState(Record.State.ACTUAL);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(recordWithoutParsedRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response putResponse = RestAssured.given()
      .spec(spec)
      .body(createdRecord.withParsedRecord(parsedEdifactRecord))
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(putResponse.statusCode(), is(HttpStatus.SC_OK));
    Record updatedRecord = putResponse.body().as(Record.class);
    assertThat(updatedRecord.getId(), is(createdRecord.getId()));
    assertThat(updatedRecord.getRawRecord().getContent(), is(rawEdifactRecord.getContent()));
    assertThat(updatedRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldUpdateErrorRecordOnPut(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
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
  public void shouldSendBadRequestWhen999ff$sIsNullDuringUpdateRecordGeneration(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId() + "/" + GENERATION)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldSendBadRequestWhenMatchedIfNotEqualTo999ff$sDuringUpdateRecordGeneration(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1.withParsedRecord(parsedMarcRecordWith001and999ff$s))
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + UUID.randomUUID() + "/" + GENERATION)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldSendNotFoundWhenUpdateRecordGenerationForNonExistingRecord(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_1.withParsedRecord(parsedMarcRecordWith001and999ff$s))
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + record_1.getMatchedId() + "/" + GENERATION)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  @Test
  public void shouldSendBadRequestWhenUpdateRecordGenerationWithDuplicate(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1.withParsedRecord(parsedMarcRecordWith001and999ff$s))
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    postSnapshots(testContext, snapshot_2);
    Record recordForUpdate = createdRecord.withSnapshotId(snapshot_2.getJobExecutionId());

    RestAssured.given()
      .spec(spec)
      .body(recordForUpdate)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getMatchedId() + "/" + GENERATION)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
    async.complete();
  }

  @Test
  public void shouldUpdateRecordGeneration(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1.withParsedRecord(parsedMarcRecordWith001and999ff$s))
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    postSnapshots(testContext, snapshot_2);
    Record recordForUpdate = createdRecord.withSnapshotId(snapshot_2.getJobExecutionId()).withGeneration(null);

    RestAssured.given()
      .spec(spec)
      .body(recordForUpdate)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getMatchedId() + "/" + GENERATION)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", not(createdRecord.getId()))
      .body("matchedId", is(recordForUpdate.getMatchedId()))
      .body("generation", is(1));
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
    postSnapshots(testContext, snapshot_2);

    Async async = testContext.async();
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
    assertThat(getRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
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
  public void shouldDeleteExistingMarcRecordOnDeleteByRecordId(TestContext testContext) {
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
    Response deletedResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId());
    Assert.assertEquals(HttpStatus.SC_OK, deletedResponse.getStatusCode());
    Record deletedRecord = deletedResponse.body().as(Record.class);

    Assert.assertEquals(true, deletedRecord.getDeleted());
    Assert.assertEquals(Record.State.DELETED, deletedRecord.getState());
    Assert.assertEquals("d", deletedRecord.getLeaderRecordStatus());
    Assert.assertEquals(true, deletedRecord.getAdditionalInfo().getSuppressDiscovery());
    Assert.assertEquals("d", ParsedRecordDaoUtil.getLeaderStatus(deletedRecord.getParsedRecord()));

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
      .body("deleted", is(true))
      .body("state", is("DELETED"))
      .body("additionalInfo.suppressDiscovery", is(true));
    async.complete();
  }

  @Test
  public void shouldDeleteExistingMarcRecordOnDeleteByInstanceIdAndUpdate005FieldWithCurrentDate(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    String srsId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String currentDate = "20240718132044.6";
    ParsedRecord parsedRecord = new ParsedRecord().withId(srsId)
      .withContent(
        new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray()
          .add(new JsonObject().put("005", currentDate))
          .add(new JsonObject().put("001", FIRST_HRID))
          .add(new JsonObject().put("999", new JsonObject()
            .put("ind1", "f")
            .put("ind2", "f")
            .put("subfields", new JsonArray()
              .add(new JsonObject().put("s", srsId))
              .add(new JsonObject().put("i", instanceId)))))).encode());

    Record newRecord = new Record()
      .withId(srsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedRecord)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId)
        .withInstanceHrid(FIRST_HRID))
      .withMatchedId(UUID.randomUUID().toString());

    Async async = testContext.async();
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + instanceId + "?idType=INSTANCE")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    async = testContext.async();
    Response deletedResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId());
    Assert.assertEquals(HttpStatus.SC_OK, deletedResponse.getStatusCode());
    Record deletedRecord = deletedResponse.body().as(Record.class);

    Assert.assertEquals(true, deletedRecord.getDeleted());
    Assert.assertEquals(Record.State.DELETED, deletedRecord.getState());
    Assert.assertEquals("d", deletedRecord.getLeaderRecordStatus());
    Assert.assertEquals(true, deletedRecord.getAdditionalInfo().getSuppressDiscovery());
    Assert.assertEquals("d", ParsedRecordDaoUtil.getLeaderStatus(deletedRecord.getParsedRecord()));

    //Complex verifying "005" field is NOT empty inside parsed record.
    LinkedHashMap<String, ArrayList<LinkedHashMap<String, String>>> content = (LinkedHashMap<String, ArrayList<LinkedHashMap<String, String>>>) deletedRecord.getParsedRecord().getContent();
    LinkedHashMap<String, String> map = content.get("fields").get(1);
    String resulted005FieldValue = map.get("005");
    Assert.assertNotNull(resulted005FieldValue);
    Assert.assertNotEquals(currentDate, resulted005FieldValue);

    async.complete();
  }

  @Test
  public void shouldReturnNoContentAndDeleteRecordIfTryingToDeleteRecordWithStateNotActual(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Record newRecord1 = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withState(Record.State.OLD)
      .withMatchedId(UUID.randomUUID().toString())
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid(FIRST_HRID));

    Async async = testContext.async();
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(newRecord1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + newRecord1.getId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();

    Record newRecord2 = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withState(Record.State.DELETED)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid(FIRST_HRID))
      .withMatchedId(UUID.randomUUID().toString());

    async = testContext.async();
    Response createParsed2 = RestAssured.given()
      .spec(spec)
      .body(newRecord2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed2.statusCode(), is(HttpStatus.SC_CREATED));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + newRecord2.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    Response deletedResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + newRecord2.getId());
    Assert.assertEquals(HttpStatus.SC_OK, deletedResponse.getStatusCode());

    Record deletedRecord = deletedResponse.body().as(Record.class);

    Assert.assertEquals(true, deletedRecord.getDeleted());
    Assert.assertEquals(Record.State.DELETED, deletedRecord.getState());
    Assert.assertEquals("d", deletedRecord.getLeaderRecordStatus());
    Assert.assertEquals(true, deletedRecord.getAdditionalInfo().getSuppressDiscovery());
    Assert.assertEquals("d", ParsedRecordDaoUtil.getLeaderStatus(deletedRecord.getParsedRecord()));

    async.complete();
  }


  @Test
  public void shouldDeleteExistingEdifactRecordOnDelete(TestContext testContext) {
    postSnapshots(testContext, snapshot_3);

    Async async = testContext.async();
    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(record_6)
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
  }

  @Test
  public void shouldReturnSortedRecordsOnGetWhenSortByOrderIsSpecified(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    postRecords(testContext, record_2, record_3, record_5);

    Async async = testContext.async();
    List<Record> records = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?snapshotId=" + snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(3))
      .body("totalRecords", is(3))
      .body("records*.deleted", everyItem(is(false)))
      .extract().response().body().as(RecordCollection.class).getRecords();

    Assert.assertEquals(11, records.get(0).getOrder().intValue());
    Assert.assertEquals(101, records.get(1).getOrder().intValue());
    Assert.assertNull(records.get(2).getOrder());

    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordIfParsedContentIsInvalid(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    Async async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_9)
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
    assertThat(getRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    assertThat(getRecord.getParsedRecord(), nullValue());
    assertThat(getRecord.getErrorRecord(), notNullValue());
    Assert.assertFalse(getRecord.getDeleted());
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestWithInvalidMarcBibRecords(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    var missing001InParsedRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(new ParsedRecord().withContent(""))
      .withState(Record.State.ACTUAL)
      .withMatchedId(UUID.randomUUID().toString())
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid(FIRST_HRID));

    var async = testContext.async();

    RestAssured.given()
      .spec(spec)
      .body(missing001InParsedRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    var missingExternalInstanceIdRecord = missing001InParsedRecord
      .withParsedRecord(marcRecordWith001)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceHrid(FIRST_HRID));

    RestAssured.given()
      .spec(spec)
      .body(missingExternalInstanceIdRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    var missingExternalInstanceHrIdRecord = missingExternalInstanceIdRecord
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID));

    RestAssured.given()
      .spec(spec)
      .body(missingExternalInstanceHrIdRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?recordType=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?state=error!")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?orderBy=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnCreatedMarcBibRecordWithAdditionalInfoOnGetById(TestContext testContext) {
    returnCreatedMarcRecordWithAdditionalInfoOnGetById(testContext, snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  public void shouldReturnCreatedMarcAuthorityRecordWithAdditionalInfoOnGetById(TestContext testContext) {
    returnCreatedMarcRecordWithAdditionalInfoOnGetById(testContext, snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  public void shouldReturnCreatedMarcHoldingsRecordWithAdditionalInfoOnGetById(TestContext testContext) {
    returnCreatedMarcRecordWithAdditionalInfoOnGetById(testContext, snapshot_4, RecordType.MARC_HOLDING);
  }

  private void returnCreatedMarcRecordWithAdditionalInfoOnGetById(TestContext testContext, Snapshot snapshot,
    RecordType marcAuthority) {
    postSnapshots(testContext, snapshot);

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(marcAuthority)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedMarcRecord)
      .withMatchedId(matchedId)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(matchedId)
        .withInstanceHrid(FIRST_HRID))
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
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    assertThat(getRecord.getId(), is(createdRecord.getId()));
    assertThat(getRecord.getRawRecord().getContent(), is(rawMarcRecord.getContent()));
    assertThat(getRecord.getAdditionalInfo().getSuppressDiscovery(),
      is(newRecord.getAdditionalInfo().getSuppressDiscovery()));
    async.complete();
  }

  @Test
  public void suppressFromDiscoveryByInstanceIdSuccess(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();
    String srsId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord().withId(srsId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray()
          .add(new JsonObject().put("001", FIRST_HRID))
          .add(new JsonObject().put("999", new JsonObject()
          .put("subfields", new JsonArray().add(new JsonObject().put("s", srsId)).add(new JsonObject().put("i", instanceId)))))));

    Record newRecord = new Record()
      .withId(srsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawMarcRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId)
        .withInstanceHrid(FIRST_HRID))
      .withMatchedId(UUID.randomUUID().toString());

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(newRecord).toString())
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("id", is(srsId));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + instanceId + "/suppress-from-discovery?idType=INSTANCE&suppress=true")
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();
  }

  @Test
  public void suppressFromDiscoveryByInstanceIdNotFound(TestContext context) {
    Async async = context.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + UUID.randomUUID().toString() + "/suppress-from-discovery?idType=INSTANCE&suppress=true")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

}
