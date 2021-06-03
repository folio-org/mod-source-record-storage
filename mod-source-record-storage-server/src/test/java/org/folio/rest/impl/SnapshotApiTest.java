package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@RunWith(VertxUnitRunner.class)
public class SnapshotApiTest extends AbstractRestVerticleTest {

  public static final String INVENTORY_INSTANCES_PATH = "/inventory/instances";

  private static Snapshot snapshot_1 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_2 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.NEW);
  private static Snapshot snapshot_3 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);
  private static Snapshot snapshot_4 = new Snapshot()
    .withJobExecutionId(UUID.randomUUID().toString())
    .withStatus(Snapshot.Status.PARSING_FINISHED);

  private static RawRecord rawRecord;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
  }

  @Before
  public void setUp(TestContext context) {
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern(INVENTORY_INSTANCES_PATH + "/.*"), true))
      .willReturn(WireMock.noContent()));

    Async async = context.async();
    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoSnapshotsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("snapshots", empty());
  }

  @Test
  public void shouldReturnAllSnapshotsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
    Snapshot[] snapshots = new Snapshot[] { snapshot_1, snapshot_2, snapshot_3 };
    postSnapshots(testContext, snapshots);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(snapshots.length));
    async.complete();
  }

  @Test
  public void shouldReturnNewSnapshotsOnGetByStatusNew(TestContext testContext) {
    postSnapshots(testContext, snapshot_1, snapshot_2, snapshot_3);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?status=" + Snapshot.Status.NEW.name())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("snapshots*.status", everyItem(is(Snapshot.Status.NEW.name())));
    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?status=error!")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?orderBy=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void shouldReturnLimitedCollectionOnGet(TestContext testContext) {
    Snapshot[] snapshots = new Snapshot[] { snapshot_1, snapshot_2, snapshot_3, snapshot_4 };
    postSnapshots(testContext, snapshots);

    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?limit=3")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("snapshots.size()", is(3))
      .body("totalRecords", is(snapshots.length));
    async.complete();
  }

  @Test
  public void shouldReturnBadRequestOnPostWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldCreateSnapshotOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_1.getJobExecutionId()))
      .body("status", is(snapshot_1.getStatus().name()));
  }

  @Test
  public void shouldReturnBadRequestOnPutWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnNotFoundOnPutWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldUpdateExistingSnapshotOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()));
    async.complete();

    async = testContext.async();
    snapshot_4.setStatus(Snapshot.Status.COMMIT_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_4.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnGetByIdWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldReturnExistingSnapshotOnGetById(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_2.getJobExecutionId()))
      .body("status", is(snapshot_2.getStatus().name()));
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_2.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_2.getJobExecutionId()))
      .body("status", is(snapshot_2.getStatus().name()));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundOnDeleteWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_3.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void shouldDeleteExistingSnapshotOnDelete(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_3)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_3.getJobExecutionId()))
      .body("status", is(snapshot_3.getStatus().name()));
    async.complete();

    String instanceId = UUID.randomUUID().toString();
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId))
      .withSnapshotId(snapshot_3.getJobExecutionId());

    List<String> recordIds = Arrays.asList(recordId, UUID.randomUUID().toString());
    for (String id : recordIds) {
      RestAssured.given()
        .spec(spec)
        .body(record.withId(id).withMatchedId(id))
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_3.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();

    for (String id : recordIds) {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(SOURCE_STORAGE_RECORDS_PATH + "/" + id)
        .then()
        .statusCode(HttpStatus.SC_NOT_FOUND);
    }
    verify(recordIds.size(), deleteRequestedFor(new UrlPathPattern(new RegexPattern(INVENTORY_INSTANCES_PATH + "/.*"), true)));
  }

  @Test
  public void shouldSetProcessingStartedDateOnPost() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_3)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_3.getJobExecutionId()))
      .body("status", is(snapshot_3.getStatus().name()))
      .body("processingStartedDate", notNullValue(Date.class));
  }

  @Test
  public void shouldSetProcessingStartedDateOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()))
      .body("processingStartedDate", nullValue(Date.class));
    async.complete();

    async = testContext.async();
    snapshot_4.setStatus(Snapshot.Status.PARSING_IN_PROGRESS);
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_4.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()))
      .body("processingStartedDate", notNullValue(Date.class));
    async.complete();
  }

}
