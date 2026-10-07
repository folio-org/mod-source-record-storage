package org.folio.rest.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

  @BeforeAll
  static void setUpClassSnapshotApi() throws IOException {
    rawRecord = new RawRecord()
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
  }

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    WireMock.stubFor(WireMock.delete(new UrlPathPattern(new RegexPattern(INVENTORY_INSTANCES_PATH + "/.*"), true))
      .willReturn(WireMock.noContent()));

    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        testContext.failNow(delete.cause());
      } else {
        testContext.completeNow();
      }
    });
  }

  @Test
  void shouldReturnEmptyListOnGetIfNoSnapshotsExist() {
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
  void shouldReturnAllSnapshotsOnGetWhenNoQueryIsSpecified() {
    Snapshot[] snapshots = new Snapshot[] { snapshot_1, snapshot_2, snapshot_3 };
    postSnapshots(snapshots);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(snapshots.length));
  }

  @Test
  void shouldReturnNewSnapshotsOnGetByStatusNew() {
    postSnapshots(snapshot_1, snapshot_2, snapshot_3);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?status=" + Snapshot.Status.NEW.name())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("snapshots*.status", everyItem(is(Snapshot.Status.NEW.name())));
  }

  @Test
  void shouldReturnErrorOnGet() {
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
  void shouldReturnLimitedCollectionOnGet() {
    Snapshot[] snapshots = new Snapshot[] { snapshot_1, snapshot_2, snapshot_3, snapshot_4 };
    postSnapshots(snapshots);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "?limit=3")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("snapshots.size()", is(3))
      .body("totalRecords", is(snapshots.length));
  }

  @Test
  void shouldReturnBadRequestOnPostWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  void shouldCreateSnapshotOnPost() {
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
  void shouldReturnBadRequestOnPutWhenNoSnapshotPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new JsonObject().toString())
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  void shouldReturnNotFoundOnPutWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .put(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldUpdateExistingSnapshotOnPut() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_4)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_4.getJobExecutionId()))
      .body("status", is(snapshot_4.getStatus().name()));

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
  }

  @Test
  void shouldReturnNotFoundOnGetByIdWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_1.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldReturnExistingSnapshotOnGetById() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_2)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_2.getJobExecutionId()))
      .body("status", is(snapshot_2.getStatus().name()));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_2.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("jobExecutionId", is(snapshot_2.getJobExecutionId()))
      .body("status", is(snapshot_2.getStatus().name()));
  }

  @Test
  void shouldReturnNotFoundOnDeleteWhenSnapshotDoesNotExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_3.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldDeleteExistingSnapshotOnDelete() {
    RestAssured.given()
      .spec(spec)
      .body(snapshot_3)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("jobExecutionId", is(snapshot_3.getJobExecutionId()))
      .body("status", is(snapshot_3.getStatus().name()));

    String recordId = UUID.randomUUID().toString();
    var marcRecordWith001 = new ParsedRecord()
      .withContent(new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("001", "id000100"))).encode());
    Record record = new Record()
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(marcRecordWith001)
      .withSnapshotId(snapshot_3.getJobExecutionId());

    List<String> recordIds = Arrays.asList(recordId, UUID.randomUUID().toString());
    for (String id : recordIds) {
      record.withId(id).withMatchedId(id)
        .withExternalIdsHolder(new ExternalIdsHolder()
          .withInstanceId(UUID.randomUUID().toString())
          .withInstanceHrid("hrid00100"));
      RestAssured.given()
        .spec(spec)
        .body(record)
        .when()
        .post(SOURCE_STORAGE_RECORDS_PATH)
        .then()
        .statusCode(HttpStatus.SC_CREATED);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_SNAPSHOTS_PATH + "/" + snapshot_3.getJobExecutionId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

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
  void shouldSetProcessingStartedDateOnPost() {
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
  void shouldSetProcessingStartedDateOnPut() {
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
  }

}
