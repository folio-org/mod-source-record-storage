package org.folio.rest.impl;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RecordsGenerationTest extends AbstractRestVerticleTest {

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

  private static String matchedId = UUID.randomUUID().toString();
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
  public void shouldCalculateRecordsGeneration(TestContext testContext) {
    List<Snapshot> snapshots = Arrays.asList(snapshot_1, snapshot_2, snapshot_3, snapshot_4);
    for (int i = 0; i < snapshots.size(); i++) {
      Async async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .body(snapshots.get(i).withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
      async.complete();

      async = testContext.async();

      Record record = new Record()
        .withId(matchedId)
        .withSnapshotId(snapshots.get(i).getJobExecutionId())
        .withRecordType(Record.RecordType.MARC_BIB)
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
        .body(snapshots.get(i).withStatus(Snapshot.Status.COMMITTED))
        .when()
        .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshots.get(i).getJobExecutionId())
        .then()
        .statusCode(HttpStatus.SC_OK);
      async.complete();

      async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + created.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(created.getId()))
        .body("rawRecord.content", is(rawRecord.getContent()))
        .body("matchedId", is(matchedId))
        .body("generation", is(i));
      async.complete();
    }
  }

  @Test
  public void shouldNotCalculateRecordGeneration(TestContext testContext){
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();

    Record record = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecord)
      .withMatchedId(matchedId)
      .withGeneration(5);

    Record created = RestAssured.given()
      .spec(spec)
      .body(record)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.COMMITTED))
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + created.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("generation", is(5));
    async.complete();
  }

  @Test
  public void shouldNotUpdateRecordsGenerationIfSnapshotsNotCommitted(TestContext testContext) {
    List<Snapshot> snapshots = Arrays.asList(snapshot_1, snapshot_2, snapshot_3, snapshot_4);
    for (Snapshot snapshot : snapshots) {
      Async async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .body(snapshot.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
      async.complete();

      async = testContext.async();
      Record record = new Record()
        .withId(matchedId)
        .withSnapshotId(snapshot.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC_BIB)
        .withRawRecord(rawRecord)
        .withParsedRecord(marcRecord)
        .withMatchedId(matchedId);

      Record created = RestAssured.given()
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
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + created.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(created.getId()))
        .body("rawRecord.content", is(rawRecord.getContent()))
        .body("matchedId", is(matchedId))
        .body("generation", is(0));
      async.complete();
    }
  }

  @Test
  public void shouldNotUpdateRecordsGenerationIfSnapshotsCommittedAfter(TestContext testContext) {
    List<Snapshot> snapshots = Arrays.asList(snapshot_1, snapshot_2);
    List<String> ids = new ArrayList<>();
    // create snapshots and records
    for (int i = 0; i < snapshots.size(); i++) {
      Async async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .body(snapshots.get(i).withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
        .when()
        .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
      async.complete();

      Record record = new Record()
        .withId(matchedId)
        .withSnapshotId(snapshots.get(i).getJobExecutionId())
        .withRecordType(Record.RecordType.MARC_BIB)
        .withRawRecord(rawRecord)
        .withParsedRecord(marcRecord)
        .withMatchedId(matchedId);

      ids.add(record.getId());

      async = testContext.async();
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
        .body("matchedId", is(matchedId))
        .body("generation", is(0));
      async.complete();
    }

    // update snapshots to committed after
    for (int i = 0; i < snapshots.size(); i++) {
      Async async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .body(snapshots.get(i).withStatus(Snapshot.Status.COMMITTED))
        .when()
        .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshots.get(i).getJobExecutionId())
        .then()
        .statusCode(HttpStatus.SC_OK);
      async.complete();

      async = testContext.async();
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + ids.get(i))
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("id", is(ids.get(i)))
        .body("rawRecord.content", is(rawRecord.getContent()))
        .body("matchedId", is(matchedId))
        .body("generation", is(0));
      async.complete();
    }
  }

  @Test
  public void shouldReturnNotFoundIfSnapshotDoesNotExist(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Record record_1 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
    async.complete();

    async = testContext.async();
    Record record_2 = new Record()
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestIfProcessingDateIsNull(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.NEW))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Record record_1 = new Record()
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
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
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnGetFormattedBySRSIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + UUID.randomUUID().toString() + "/formatted")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnNotFoundOnGetFormattedByInstanceIdWhenRecordDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + UUID.randomUUID().toString() + "/formatted?idType=INSTANCE")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnSameRecordOnGetByIdAndGetBySRSId(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    String srsId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord().withId(srsId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields", new JsonArray().add(new JsonObject().put("s", srsId)))))));

    Record newRecord = new Record()
      .withId(srsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(srsId);

    RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("id", is(srsId));
    async.complete();

    async = testContext.async();
    Record getByIdRecord = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + srsId)
      .body().as(Record.class);

    Record getBySRSIdRecord = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + srsId + "/formatted")
      .body().as(Record.class);

    MatcherAssert.assertThat(getByIdRecord.getId(), is(getBySRSIdRecord.getId()));
    MatcherAssert.assertThat(getByIdRecord.getRawRecord().getContent(), is(getBySRSIdRecord.getRawRecord().getContent()));
    MatcherAssert.assertThat(getBySRSIdRecord.getParsedRecord().getFormattedContent(), notNullValue());
    MatcherAssert.assertThat(getBySRSIdRecord.getParsedRecord().getFormattedContent(), containsString("LEADER 01542ccm a2200361   4500"));
    async.complete();
  }

  @Test
  public void shouldReturnRecordOnGetByInstanceId(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1.withStatus(Snapshot.Status.PARSING_IN_PROGRESS))
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    String srsId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    ParsedRecord parsedRecord = new ParsedRecord().withId(srsId)
      .withContent(new JsonObject().put("leader", "01542ccm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields", new JsonArray().add(new JsonObject().put("s", srsId)).add(new JsonObject().put("i", instanceId)))))));

    Record newRecord = new Record()
      .withId(srsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(matchedId)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId));

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
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + instanceId + "/formatted?idType=INSTANCE")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("parsedRecord.content", notNullValue());
    async.complete();
  }

}
