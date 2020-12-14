package org.folio.rest.impl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.folio.TestMocks;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SourceStorageBatchApiTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_BATCH_RECORDS_PATH = "/source-storage/batch/records";
  private static final String SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH = "/source-storage/batch/parsed-records";

  private static final String INVALID_POST_REQUEST = "{\"records\":[{\"id\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"snapshotId\":\"7f939c0b-618c-4eab-8276-a14e0bfe5728\",\"matchedId\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"generation\":0,\"recordType\":\"MARC\",\"rawRecord\":{\"id\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"content\":\"01104cam \"},\"parsedRecord\":{\"id\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"content\":{\"leader\":\"00000cam a2200277   4500\",\"fields\":[{\"001\":\"in00000000007\"},{\"005\":\"20120817205822.0\"},{\"008\":\"690410s1965    dcu          f000 0 eng  \"},{\"010\":{\"subfields\":[{\"a\":\"65062892\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"600\":{\"subfields\":[{\"a\":\"Ross, Arthur M.\"},{\"q\":\"(Arthur Max),\"},{\"d\":\"1916-1970.\"},{\"0\":\"http://id.loc.gov/authorities/names/n50047449\"}],\"ind1\":\"1\",\"ind2\":\"0\"}}],\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"i\":\"5b38b5e6-dfa3-4f51-8b7f-858421310aa7\"}]}}},\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"5b38b5e6-dfa3-4f51-8b7f-858421310aa7\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\"}],\"totalRecords\":1}";
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

  private static Snapshot snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

  private static ErrorRecord errorRecord = new ErrorRecord()
    .withDescription("Oops... something happened")
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");

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
  private static Record record_4 = new Record()
    .withId(FOURTH_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(FOURTH_UUID)
    .withOrder(1)
    .withState(Record.State.ACTUAL);

  @Before
  public void setUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      SnapshotDaoUtil.save(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID), TestMocks.getSnapshots()).onComplete(save -> {
        if (save.failed()) {
          context.fail(save.cause());
        }
        async.complete();
      });
    });
  }

  @Test
  public void shouldPostSourceStorageBatchRecords(TestContext testContext) {
    Async async = testContext.async();
    List<Record> expected = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    recordCollection.getRecords()
      .forEach(record -> record.withMetadata(null));
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(10))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(10));
    async.complete();
  }

  @Test
  public void shouldPostWithoutErrorsSourceStorageBatchRecordsWithInvalidRecord(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(INVALID_POST_REQUEST)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(1))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(1));
    async.complete();
  }

  @Test
  public void shouldCreateRecordsOnPostRecordCollection(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record_1, record_4))
      .withTotalRecords(2);

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records*.snapshotId", everyItem(is(snapshot_1.getJobExecutionId())))
      .body("records*.recordType", everyItem(is(record_1.getRecordType().name())))
      .body("records*.rawRecord.content", notNullValue())
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)))
      .body("records*.metadata", notNullValue())
      .body("records*.metadata.createdDate", notNullValue(String.class))
      .body("records*.metadata.createdByUserId", notNullValue(String.class))
      .body("records*.metadata.updatedDate", notNullValue(String.class))
      .body("records*.metadata.updatedByUserId", notNullValue(String.class));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoRecordsInRecordCollection() {
    RecordCollection recordCollection = new RecordCollection();
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateRawRecordAndErrorRecordOnPostInRecordCollection(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record_2, record_3))
      .withTotalRecords(2);

    async = testContext.async();
    RecordsBatchResponse createdRecordCollection = RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract().response().body().as(RecordsBatchResponse.class);

    Record createdRecord = createdRecordCollection.getRecords().get(0);
    assertThat(createdRecord.getId(), notNullValue());
    assertThat(createdRecord.getSnapshotId(), is(record_2.getSnapshotId()));
    assertThat(createdRecord.getRecordType(), is(record_2.getRecordType()));
    assertThat(createdRecord.getRawRecord().getContent(), is(record_2.getRawRecord().getContent()));
    assertThat(createdRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));

    createdRecord = createdRecordCollection.getRecords().get(1);
    assertThat(createdRecord.getId(), notNullValue());
    assertThat(createdRecord.getSnapshotId(), is(record_3.getSnapshotId()));
    assertThat(createdRecord.getRecordType(), is(record_3.getRecordType()));
    assertThat(createdRecord.getRawRecord().getContent(), is(record_3.getRawRecord().getContent()));
    assertThat(createdRecord.getErrorRecord().getContent(), is(record_3.getErrorRecord().getContent()));
    assertThat(createdRecord.getAdditionalInfo().getSuppressDiscovery(), is(false));
    async.complete();
  }

  @Test
  public void shouldCreateRecordsWithFilledMetadataWhenUserIdHeaderIsAbsent(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record_1, record_4))
      .withTotalRecords(2);

    async = testContext.async();
    RestAssured.given()
      .spec(specWithoutUserId)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records*.snapshotId", everyItem(is(snapshot_1.getJobExecutionId())))
      .body("records*.recordType", everyItem(is(record_1.getRecordType().name())))
      .body("records*.rawRecord.content", notNullValue())
      .body("records*.additionalInfo.suppressDiscovery", everyItem(is(false)))
      .body("records*.metadata", notNullValue())
      .body("records*.metadata.createdDate", notNullValue(String.class))
      .body("records*.metadata.createdByUserId", notNullValue(String.class))
      .body("records*.metadata.updatedDate", notNullValue(String.class))
      .body("records*.metadata.updatedByUserId", notNullValue(String.class));
    async.complete();
  }

  @Test
  public void shouldPutSourceStorageBatchParsedRecords(TestContext testContext) {
    Async async = testContext.async();
    List<Record> original = TestMocks.getRecords();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    recordCollection.getRecords()
      .forEach(record -> record.withMetadata(null));
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(10))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(10));
    async.complete();

    async = testContext.async();
    List<Record> updated = original.stream()
      .map(record -> record.withExternalIdsHolder(record.getExternalIdsHolder().withInstanceId(UUID.randomUUID().toString())))
      .collect(Collectors.toList());
    recordCollection
      .withRecords(updated)
      .withTotalRecords(updated.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("parsedRecords.size()", is(10))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(10));
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

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
      .withState(Record.State.ACTUAL)
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

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Collections.singletonList(createdRecord))
      .withTotalRecords(1);

    async = testContext.async();
    recordCollection.getRecords()
      .forEach(record -> record.withMetadata(null));
    ParsedRecordsBatchResponse updatedParsedRecordCollection = RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(ParsedRecordsBatchResponse.class);

    ParsedRecord updatedParsedRecord = updatedParsedRecordCollection.getParsedRecords().get(0);
    assertThat(updatedParsedRecord.getId(), notNullValue());
    assertThat(JsonObject.mapFrom(updatedParsedRecord).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    async.complete();

    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId())
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("metadata", notNullValue())
      .body("metadata.createdDate", notNullValue(String.class))
      .body("metadata.createdByUserId", notNullValue(String.class))
      .body("metadata.updatedDate", notNullValue(String.class))
      .body("metadata.updatedByUserId", notNullValue(String.class));
  }

  @Test
  public void shouldReturnBadRequestOnUpdateParsedRecordsIfNoIdPassed(TestContext testContext) {
    Async async = testContext.async();
    Record record1 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent())
        .withId(UUID.randomUUID().toString()));

    Record record2 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent()));

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record1, record2))
      .withTotalRecords(2);

    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
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

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Collections.singletonList(createdRecord.withParsedRecord(parsedRecordJson)))
      .withTotalRecords(1);

    async = testContext.async();
    recordCollection.getRecords()
      .forEach(record -> record.withMetadata(null));
    ParsedRecordsBatchResponse updatedParsedRecordCollection = RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().response().body().as(ParsedRecordsBatchResponse.class);

    ParsedRecord updatedParsedRecord = updatedParsedRecordCollection.getParsedRecords().get(0);
    assertThat(updatedParsedRecord.getId(), notNullValue());
    assertThat(JsonObject.mapFrom(updatedParsedRecord).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    async.complete();
  }

  @Test
  public void shouldReturnErrorMessagesOnUpdateParsedRecordsIfIdIsNotFound(TestContext testContext) {
    Async async = testContext.async();

    Record record1 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent())
        .withId(UUID.randomUUID().toString()));

    Record record2 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withRawRecord(rawRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent())
        .withId(UUID.randomUUID().toString()));

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record1, record2))
      .withTotalRecords(2);

    ParsedRecordsBatchResponse result = RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .extract().response().body().as(ParsedRecordsBatchResponse.class);

    assertThat(result.getErrorMessages(), hasSize(2));
    async.complete();
  }

}
