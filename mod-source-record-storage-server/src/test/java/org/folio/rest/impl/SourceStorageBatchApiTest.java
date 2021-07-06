package org.folio.rest.impl;

import static java.lang.String.format;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
import org.folio.rest.jaxrs.model.Record.RecordType;
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

  private static final String INVALID_POST_REQUEST = "{\"records\":[{\"id\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"snapshotId\":\"7f939c0b-618c-4eab-8276-a14e0bfe5728\",\"matchedId\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"generation\":0,\"recordType\":\"MARC_BIB\",\"rawRecord\":{\"id\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"content\":\"01104cam \"},\"parsedRecord\":{\"id\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"content\":{\"leader\":\"00000cam a2200277   4500\",\"fields\":[{\"001\":\"in00000000007\"},{\"005\":\"20120817205822.0\"},{\"008\":\"690410s1965    dcu          f000 0 eng  \"},{\"010\":{\"subfields\":[{\"a\":\"65062892\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"600\":{\"subfields\":[{\"a\":\"Ross, Arthur M.\"},{\"q\":\"(Arthur Max),\"},{\"d\":\"1916-1970.\"},{\"0\":\"http://id.loc.gov/authorities/names/n50047449\"}],\"ind1\":\"1\",\"ind2\":\"0\"}}],\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"96fbcc07-d67e-47bd-900d-90ae261edb73\",\"i\":\"5b38b5e6-dfa3-4f51-8b7f-858421310aa7\"}]}}},\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"5b38b5e6-dfa3-4f51-8b7f-858421310aa7\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\"}],\"totalRecords\":1}";
  private static final String FIRST_UUID = UUID.randomUUID().toString();
  private static final String SECOND_UUID = UUID.randomUUID().toString();
  private static final String THIRD_UUID = UUID.randomUUID().toString();
  private static final String FOURTH_UUID = UUID.randomUUID().toString();
  private static final String FIFTH_UUID = UUID.randomUUID().toString();

  private static RawRecord rawRecord;
  private static ParsedRecord marcRecord;

  static {
    try {
      rawRecord = new RawRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
      marcRecord = new ParsedRecord()
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
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
  private static Snapshot snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

  private static ErrorRecord errorRecord = new ErrorRecord()
    .withDescription("Oops... something happened")
    .withContent("Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.");

  private static Record record_1 = new Record()
    .withId(FIRST_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withMatchedId(FIRST_UUID)
    .withOrder(0)
    .withState(Record.State.ACTUAL);
  private static Record record_2 = new Record()
    .withId(SECOND_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(SECOND_UUID)
    .withOrder(11)
    .withState(Record.State.ACTUAL);
  private static Record record_3 = new Record()
    .withId(THIRD_UUID)
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withErrorRecord(errorRecord)
    .withMatchedId(THIRD_UUID)
    .withState(Record.State.ACTUAL);
  private static Record record_4 = new Record()
    .withId(FOURTH_UUID)
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC_BIB)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(FOURTH_UUID)
    .withOrder(1)
    .withState(Record.State.ACTUAL);
  private static Record record_5 = new Record()
    .withId(FIFTH_UUID)
    .withSnapshotId(snapshot_3.getJobExecutionId())
    .withRecordType(RecordType.MARC_AUTHORITY)
    .withRawRecord(rawRecord)
    .withParsedRecord(marcRecord)
    .withMatchedId(FIFTH_UUID)
    .withOrder(0)
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
  public void shouldPostSourceStorageBatchMarcRecords(TestContext testContext) {
    Async async = testContext.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC_BIB))
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(expected.size()))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(expected.size()));
    async.complete();
  }

  @Test
  public void shouldPostSourceStorageBatchEdifactRecords(TestContext testContext) {
    Async async = testContext.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.EDIFACT))
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(expected.size()))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(expected.size()));
    async.complete();
  }

  @Test
  public void shouldFailWhenPostSourceStorageBatchRecordsWithMultipleSnapshots(TestContext testContext) {
    Async async = testContext.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC_BIB))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST)
      .body(is("Batch record collection only supports single snapshot"));
    async.complete();
  }

  @Test
  public void shouldFailWhenPostSourceStorageBatchRecordsWithMultipleRecordTypes(TestContext testContext) {
    Async async = testContext.async();
    List<Record> expected = TestMocks.getRecords().stream()
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST)
      .body(is("Batch record collection only supports single record type"));
    async.complete();
  }

  @Test
  public void shouldPostSourceStorageBatchRecordsCalculateRecordsGeneration(TestContext testContext) {
    Snapshot snapshot1 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    Snapshot snapshot2 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    Snapshot snapshot3 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    Snapshot snapshot4 = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    List<Snapshot> snapshots = Arrays.asList(snapshot1, snapshot2, snapshot3, snapshot4);

    List<Record> records = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC_BIB))
      .map(record -> {
        RawRecord rawRecord = record.getRawRecord();
        if (Objects.nonNull(rawRecord)) {
          rawRecord.setId(null);
        }
        ParsedRecord parsedRecord = record.getParsedRecord();
        if (Objects.nonNull(parsedRecord)) {
          parsedRecord.setId(null);
        }
        ErrorRecord errorRecord = record.getErrorRecord();
        if (Objects.nonNull(errorRecord)) {
          errorRecord.setId(null);
        }
        return record
          .withId(null)
          .withRawRecord(rawRecord)
          .withParsedRecord(parsedRecord)
          .withErrorRecord(errorRecord);
      })
      .collect(Collectors.toList());

    List<String> previousRecordIds = new ArrayList<>();

    for (int i = 0; i < snapshots.size(); i++) {
      final Snapshot snapshot = snapshots.get(i);
      Async async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .body(snapshot.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
      async.complete();

      records = records.stream()
        .map(record -> record.withSnapshotId(snapshot.getJobExecutionId()))
        .collect(Collectors.toList());

      RecordCollection recordCollection = new RecordCollection()
        .withRecords(records)
        .withTotalRecords(records.size());
      RecordsBatchResponse response = RestAssured.given()
        .spec(spec)
        .body(recordCollection)
        .when()
        .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
        .body().as(RecordsBatchResponse.class);

      testContext.assertEquals(records.size(), response.getRecords().size());
      testContext.assertEquals(0, response.getErrorMessages().size());
      testContext.assertEquals(records.size(), response.getTotalRecords());

      async = testContext.async();

      RestAssured.given()
        .spec(spec)
        .body(snapshot.withStatus(Snapshot.Status.COMMITTED))
        .when()
        .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot.getJobExecutionId())
        .then()
        .statusCode(HttpStatus.SC_OK);
      async.complete();

      if (!previousRecordIds.isEmpty()) {
        // assert old records state and generation
        for (String recordId : previousRecordIds) {
          async = testContext.async();
          RestAssured.given()
            .spec(spec)
            .when()
            .get(SOURCE_STORAGE_RECORDS_PATH + "/" + recordId)
            .then()
            .statusCode(HttpStatus.SC_OK)
            .body("id", is(recordId))
            .body("state", is(Record.State.OLD.name()))
            .body("generation", is(i - 1));
          async.complete();
        }
        previousRecordIds.clear();
      }

      // assert new records state and generation
      for (Record record : response.getRecords()) {
        async = testContext.async();
        RestAssured.given()
          .spec(spec)
          .when()
          .get(SOURCE_STORAGE_RECORDS_PATH + "/" + record.getId())
          .then()
          .statusCode(HttpStatus.SC_OK)
          .body("id", is(record.getId()))
          .body("matchedId", is(record.getMatchedId()))
          .body("state", is(Record.State.ACTUAL.name()))
          .body("generation", is(i));

        previousRecordIds.add(record.getId());
        async.complete();
      }
    }
  }

  @Test
  public void shouldFailWithSnapshotNotFoundException(TestContext testContext) {
    Async async = testContext.async();
    String snapshotId = "c698cfde-14e1-4edf-8b54-d9d43895571e";
    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC_BIB))
      .map(record -> record.withSnapshotId(snapshotId))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .body(is(format(SNAPSHOT_NOT_FOUND_TEMPLATE, snapshotId)));
    async.complete();
  }

  @Test
  public void shouldFailWithInvalidSnapshotStatusBadRequest(TestContext testContext) {
    Async async = testContext.async();
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.NEW);

    RestAssured.given()
      .spec(spec)
      .body(snapshot)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot.getJobExecutionId()))
      .body("status", is(snapshot.getStatus().name()));

    async.complete();
    async = testContext.async();

    List<Record> expected = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC_BIB))
      .map(record -> record.withSnapshotId(snapshot.getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST)
      .body(is(format(SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE, snapshot.getStatus())));
    async.complete();
  }

  @Test
  public void shouldPostSourceStorageBatchRecordsWithInvalidRecord(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(INVALID_POST_REQUEST)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(1))
      .body("errorMessages.size()", is(1))
      .body("totalRecords", is(1));
    async.complete();
  }

  @Test
  public void shouldCreateRecordsOnPostRecordCollection(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record_1, record_4))
      .withTotalRecords(2);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
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
  public void shouldReturnBadRequestOnPostWhenNoRecordsInRecordCollection(TestContext testContext) {
    Async async = testContext.async();
    RecordCollection recordCollection = new RecordCollection();
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    async.complete();
  }

  @Test
  public void shouldCreateRawRecordAndErrorRecordOnPostInRecordCollection(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record_2, record_3))
      .withTotalRecords(2);

    Async async = testContext.async();
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
    postSnapshots(testContext, snapshot_1);

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record_1, record_4))
      .withTotalRecords(2);

    Async async = testContext.async();
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
    List<Record> original = TestMocks.getRecords().stream()
      .filter(record -> record.getRecordType().equals(RecordType.MARC_BIB))
      .map(record -> record.withSnapshotId(TestMocks.getSnapshot(0).getJobExecutionId()))
      .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(original)
      .withTotalRecords(original.size());
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(original.size()))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(original.size()));
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
      .body("parsedRecords.size()", is(updated.size()))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(updated.size()));
    async.complete();
  }

  @Test
  public void shouldUpdateParsedRecords(TestContext testContext) {
    postSnapshots(testContext, snapshot_2);

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
      .withState(Record.State.ACTUAL)
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(false));

    Async async = testContext.async();
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
      .then()
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
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent())
        .withId(UUID.randomUUID().toString()));

    Record record2 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
    postSnapshots(testContext, snapshot_2);

    Record newRecord = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(false));

    Async async = testContext.async();
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
  public void shouldReturnErrorMessagesOnUpdateParsedRecordsIfRecordIdNotFound(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();

    Record record1 = new Record()
      .withId(UUID.randomUUID().toString())
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent()));

    Record record2 = new Record()
      .withId(UUID.randomUUID().toString())
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withContent(marcRecord.getContent()));

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record1, record2))
      .withTotalRecords(2);

    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(recordCollection.getRecords().size()))
      .body("totalRecords", is(recordCollection.getRecords().size()))
      .body("errorMessages.size()", is(0));
    async.complete();

    async = testContext.async();

    record1.setParsedRecord(new ParsedRecord()
      .withContent(marcRecord.getContent())
      .withId(record1.getId()));

    record2.setParsedRecord(new ParsedRecord()
      .withContent(marcRecord.getContent())
      .withId(record2.getId()));

    recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record1.withId(UUID.randomUUID().toString()), record2.withId(UUID.randomUUID().toString())))
      .withTotalRecords(2);

    ParsedRecordsBatchResponse updatedParsedRecordCollection = RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("parsedRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("errorMessages.size()", is(2))
      .extract().response().body().as(ParsedRecordsBatchResponse.class);

    testContext.assertEquals(marcRecord.getContent(), updatedParsedRecordCollection.getParsedRecords().get(0).getContent());
    testContext.assertEquals(marcRecord.getContent(), updatedParsedRecordCollection.getParsedRecords().get(1).getContent());

    testContext.assertEquals(format("Record with id %s was not updated", record1.getId()), updatedParsedRecordCollection.getErrorMessages().get(0));
    testContext.assertEquals(format("Record with id %s was not updated", record2.getId()), updatedParsedRecordCollection.getErrorMessages().get(1));

    async.complete();
  }

  @Test
  public void shouldReturnErrorMessagesOnUpdateParsedRecordsIfParsedRecordIdNotFound(TestContext testContext) {
    postSnapshots(testContext, snapshot_1);

    Async async = testContext.async();

    Record record1 = new Record()
      .withId(UUID.randomUUID().toString())
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord);

    Record record2 = new Record()
      .withId(UUID.randomUUID().toString())
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord);

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record1, record2))
      .withTotalRecords(2);

    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(SOURCE_STORAGE_BATCH_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("records.size()", is(recordCollection.getRecords().size()))
      .body("errorMessages.size()", is(0))
      .body("totalRecords", is(recordCollection.getRecords().size()));
    async.complete();

    async = testContext.async();

    record1.setParsedRecord(new ParsedRecord()
      .withContent(marcRecord.getContent())
      .withId(UUID.randomUUID().toString()));

    record2.setParsedRecord(new ParsedRecord()
      .withContent(marcRecord.getContent())
      .withId(UUID.randomUUID().toString()));

    recordCollection = new RecordCollection()
      .withRecords(Arrays.asList(record1, record2))
      .withTotalRecords(2);

    ParsedRecordsBatchResponse updatedParsedRecordCollection = RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .body("errorMessages.size()", is(2))
      .body("parsedRecords.size()", is(0))
      .body("totalRecords", is(0))
      .extract().response().body().as(ParsedRecordsBatchResponse.class);

    testContext.assertEquals(format("Parsed Record with id '%s' was not updated", record1.getParsedRecord().getId()), updatedParsedRecordCollection.getErrorMessages().get(0));
    testContext.assertEquals(format("Parsed Record with id '%s' was not updated", record2.getParsedRecord().getId()), updatedParsedRecordCollection.getErrorMessages().get(1));

    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoParsedRecordsInRecordCollection(TestContext testContext) {
    Async async = testContext.async();
    RecordCollection recordCollection = new RecordCollection();
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .put(SOURCE_STORAGE_BATCH_PARSED_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    async.complete();
  }

}
