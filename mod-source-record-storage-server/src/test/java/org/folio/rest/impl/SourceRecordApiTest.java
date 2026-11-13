package org.folio.rest.impl;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.services.AbstractLBServiceTest.getFullModuleName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxTestContext;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.IdType;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.client.TenantClient;
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
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  private static final String FIRST_HRID = "hridFirst";
  private static final String SECOND_HRID = "hridSecond";
  private static final String THIRD_HRID = "hridThird";

  private static final String CENTRAL_TENANT_ID = "consortium";
  private static final String CONSORTIUM_ID = "consortiumIds";

  private static final RawRecord rawRecord;
  private static final ParsedRecord parsedRecord;
  private static final ParsedRecord parsedRecordWith001;
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
      parsedRecord = new ParsedRecord()
        .withContent(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH));
      parsedRecordWith001 = new ParsedRecord()
        .withContent(new JsonObject().put("fields", new JsonArray().add(new JsonObject().put("001", FIRST_HRID))).encode());
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
      .withParsedRecord(parsedRecordWith001)
      .withMatchedId(FIRST_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_2 = new Record()
      .withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
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
      .withParsedRecord(parsedRecordWith001)
      .withMatchedId(THIRD_UUID)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_4 = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_5 = new Record()
      .withId(FIFTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withMatchedId(FIFTH_UUID)
      .withParsedRecord(invalidParsedRecord)
      .withOrder(101)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(UUID.randomUUID().toString())
        .withInstanceHrid("12345"));
    record_6 = new Record()
      .withId(SIXTH_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withMatchedId(SIXTH_UUID)
      .withParsedRecord(parsedRecord)
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
      .withParsedRecord(parsedRecord)
      .withMatchedId(EIGHTH_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityId(UUID.randomUUID().toString())
        .withAuthorityHrid("12345"));
    record_9 = new Record()
      .withId(NINTH_UUID)
      .withSnapshotId(snapshot_5.getJobExecutionId())
      .withRecordType(RecordType.MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(NINTH_UUID)
      .withOrder(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withHoldingsId(UUID.randomUUID().toString())
        .withHoldingsHrid("12345"));
  }

  @BeforeAll
  static void setUpBeforeClass(VertxTestContext testContext) {
    TenantClient tenantClient =
      new TenantClient(OKAPI_URL, CENTRAL_TENANT_ID, OKAPI_TOKEN, WebClient.create(vertx.getDelegate()));

    try {
      tenantClient.postTenant(new TenantAttributes().withModuleTo(getFullModuleName()), ar -> {
        if (!ar.succeeded()) {
          testContext.failNow(ar.cause());
          return;
        }
        if (ar.result().statusCode() == 204) {
          testContext.completeNow();
          return;
        }
        if (ar.result().statusCode() == 201) {
          tenantClient.getTenantByOperationId(ar.result().bodyAsJson(TenantJob.class).getId(), 60000, resp -> {
            if (!resp.succeeded()) {
              testContext.failNow(resp.cause());
              return;
            }
            String error = resp.result().bodyAsJson(TenantJob.class).getError();
            if (error != null && !error.contains("EventDescriptor was not registered for eventType")) {
              testContext.failNow(new RuntimeException(error));
              return;
            }
            testContext.completeNow();
          });
        } else {
          testContext.failNow(new RuntimeException("Failed to make post tenant. Received status code 400"));
        }
      });
    } catch (Exception e) {
      testContext.failNow(e);
    }
  }

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_PATH), false))
      .willReturn(WireMock.ok()
        .withBody(new JsonObject()
          .put("userTenants", JsonArray.of(
            new JsonObject()
              .put("centralTenantId", CENTRAL_TENANT_ID)
              .put("consortiumId", CONSORTIUM_ID)))
          .encode()
        )));

    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID))
      .compose(v -> SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, CENTRAL_TENANT_ID)))
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldReturnSpecificMarcSourceRecordOnGetByRecordId() {
    postSnapshots(snapshot_1, snapshot_2, snapshot_3);

    postRecords(record_1, record_3, record_7);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
  }

  @Test
  void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordId() {
    shouldReturnSpecificMarcRecordSourceRecordOnGetByRecordId(RecordType.MARC_AUTHORITY, record_8, snapshot_4);
  }

  @Test
  void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByRecordId() {
    shouldReturnSpecificMarcRecordSourceRecordOnGetByRecordId(RecordType.MARC_HOLDING, record_9, snapshot_5);
  }

  @Test
  void shouldReturnSpecificEdifactSourceRecordOnGetByRecordId() {
    postSnapshots(snapshot_1, snapshot_2, snapshot_3);

    postRecords(record_1, record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_7)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(
        SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=EDIFACT&recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
  }

  @Test
  void shouldReturnSpecificMarcBibSourceRecordOnGetByDefaultExternalId() {
    returnSpecificMarcSourceRecordOnGetByDefaultExternalId(snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByDefaultExternalId() {
    returnSpecificMarcSourceRecordOnGetByDefaultExternalId(snapshot_5, RecordType.MARC_HOLDING);
  }

  @Test
  void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByDefaultExternalId() {
    returnSpecificMarcSourceRecordOnGetByDefaultExternalId(snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldReturnSpecificSourceRecordOnGetByInstanceExternalId() {
    returnSpecificMarcSourceRecordOnGetByExternalId(snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByHoldingsExternalId() {
    returnSpecificMarcSourceRecordOnGetByExternalId(snapshot_5, RecordType.MARC_HOLDING);
  }

  @Test
  void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByHoldingsExternalId() {
    returnSpecificMarcSourceRecordOnGetByExternalId(snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldReturnSpecificNumberOfMarcBibSourceRecordsOnGetByInstanceExternalHrid() {
    returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(snapshot_2, RecordType.MARC_BIB,
      "?instanceHrid=");
  }

  @Test
  void shouldReturnSpecificNumberOfMarcHoldingsSourceRecordsOnGetByHoldingsExternalHrid() {
    returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(snapshot_5,
      RecordType.MARC_HOLDING, "?recordType=MARC_HOLDING&holdingsHrid=");
  }

  @Test
  void shouldReturnSpecificNumberOfMarcHoldingsSourceRecordsOnGetByInstanceExternalHrid() {
    returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(snapshot_5,
      RecordType.MARC_HOLDING, "?recordType=MARC_HOLDING&externalHrid=");
  }

  @Test
  void shouldReturnSpecificMarcBibSourceRecordOnGetByRecordExternalId() {
    returnSpecificMarcSourceRecordOnGetByRecordExternalId(snapshot_2, RecordType.MARC_BIB);
  }

  @Test
  void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordExternalId() {
    returnSpecificMarcSourceRecordOnGetByRecordExternalId(snapshot_4, RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldReturnSpecificMarcHoldingSourceRecordOnGetByRecordExternalId() {
    returnSpecificMarcSourceRecordOnGetByRecordExternalId(snapshot_5, RecordType.MARC_HOLDING);
  }

  @Test
  void shouldReturnSpecificMarcBibSourceRecordOnGetByRecordExternalIdAndRecordState() {
    returnSpecificMarcSourceRecordOnGetByRecordExternalIdAndRecordState(snapshot_2, RecordType.MARC_BIB, Record.State.DELETED);
  }

  @Test
  void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordExternalIdAndRecordState() {
    returnSpecificMarcSourceRecordOnGetByRecordExternalIdAndRecordState(snapshot_4, RecordType.MARC_AUTHORITY, Record.State.DRAFT);
  }

  @Test
  void shouldReturnSpecificMarcHoldingSourceRecordOnGetByRecordExternalIdAndRecordState() {
    returnSpecificMarcSourceRecordOnGetByRecordExternalIdAndRecordState(snapshot_5, RecordType.MARC_HOLDING, Record.State.OLD);
  }

  @Test
  void shouldReturnSpecificMarcBibSourceRecordOnGetByRecordLeaderRecordStatus() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1, record_3);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    String leaderStatus = ParsedRecordDaoUtil.getLeaderStatus(createdRecord.getParsedRecord());

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?leaderRecordStatus=" + leaderStatus + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
  }

  @Test
  void shouldReturnSpecificMarcAuthoritySourceRecordOnGetByRecordLeaderRecordStatus() {
    shouldReturnSpecificMarcSourceRecordOnGetByRecordLeaderRecordStatus(RecordType.MARC_AUTHORITY, record_8,
      snapshot_4);
  }

  @Test
  void shouldReturnSpecificMarcHoldingsSourceRecordOnGetByRecordLeaderRecordStatus() {
    shouldReturnSpecificMarcSourceRecordOnGetByRecordLeaderRecordStatus(RecordType.MARC_HOLDING, record_9,
      snapshot_5);
  }

  @Test
  void shouldReturnBadRequestOnGetIfInvalidExternalIdType() {
    postSnapshots(snapshot_1, snapshot_2);

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(SECOND_HRID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_HRID));

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

    Record marcRecord = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid("hrid12345"));

    RestAssured.given()
      .spec(spec)
      .body(marcRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + SECOND_UUID + "?idType=invalidrecordtype")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  void shouldNotReturnSpecificSourceRecordOnGetIfItIsNotExists() {
    postSnapshots(snapshot_1, snapshot_2);

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(SECOND_HRID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_HRID));

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

    Record marcRecord = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid("hrid12345"));

    RestAssured.given()
      .spec(spec)
      .body(marcRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + FIFTH_UUID + "?idType=INSTANCE")
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  void shouldReturnDeletedRecordByExternalIdIfStateIsEmpty() {
    postSnapshots(snapshot_1, snapshot_2);

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withLeaderRecordStatus("d")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(SECOND_HRID));

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.DELETED)
      .withLeaderRecordStatus("d")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_HRID));

    Record thirdRecord = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.DELETED)
      .withLeaderRecordStatus("d")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(THIRD_UUID).withInstanceHrid(THIRD_HRID));

    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(firstRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));

    createResponse = RestAssured.given()
      .spec(spec)
      .body(secondRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));

    createResponse = RestAssured.given()
      .spec(spec)
      .body(thirdRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + FIRST_UUID + "?idType=EXTERNAL")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("recordId", is(SECOND_UUID))
      .body("recordType", is(Record.RecordType.MARC_BIB.value()))
      .body("externalIdsHolder.instanceId", is(FIRST_UUID))
      .body("order", is(11));
  }

  @Test
  void shouldReturnEmptyCollectionOnGetByRecordIdIfParsedRecordIsIncorrect() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1, record_5);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_5)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
  }

  @Test
  void shouldReturnEmptyCollectionOnGetByRecordIdIfThereIsNoSuchRecord() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1, record_2, record_3);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + UUID.randomUUID() + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
  }

  @Test
  void shouldReturnEmptyCollectionOnGetByRecordIdAndRecordStateActualIfRecordWasDeleted() {
    postSnapshots(snapshot_2);

    Response createParsed = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    Record parsed = createParsed.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsed.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + parsed.getId() + "&recordState=ACTUAL&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(0))
      .body("totalRecords", is(0));
  }

  @Test
  void shouldReturnBadRequestOnGetByRecordIdIfInvalidUUID() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1, record_2);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordId=" + createdRecord.getId().substring(1).replace("-", "")
        + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  void shouldReturnSortedSourceRecordsOnGetWhenSortByIsSpecified() {
    postSnapshots(snapshot_1, snapshot_2, snapshot_3);

    String firstMatchedId = UUID.randomUUID().toString();

    Record tmpRecord4 = new Record()
      .withId(firstMatchedId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(firstMatchedId)
      .withOrder(1)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_HRID));

    String secondMathcedId = UUID.randomUUID().toString();

    Record tmpRecord2 = new Record()
      .withId(secondMathcedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(secondMathcedId)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(SECOND_HRID));

    postRecords(record_2, tmpRecord2, record_4, tmpRecord4, record_7);

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

    assertTrue(
      sourceRecordList.get(0).getMetadata().getCreatedDate().after(sourceRecordList.get(1).getMetadata().getCreatedDate()));
    assertTrue(
      sourceRecordList.get(1).getMetadata().getCreatedDate().after(sourceRecordList.get(2).getMetadata().getCreatedDate()));
    assertTrue(
      sourceRecordList.get(2).getMetadata().getCreatedDate().after(sourceRecordList.get(3).getMetadata().getCreatedDate()));
  }

  @Test
  void shouldReturnSortedMarcBibSourceRecordsOnGetWhenSortByOrderIsSpecified() {
    postSnapshots(snapshot_2, snapshot_3);

    postRecords(record_2, record_3, record_5, record_6, record_7);

    // NOTE: get source records will not return if there is no associated parsed record
    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + snapshot_2.getJobExecutionId() + "&orderBy=order")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(3))
      .body("sourceRecords*.recordType", everyItem(is(RecordType.MARC_BIB.name())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    assertThat(sourceRecordList.get(0).getOrder(), is(11));
    assertThat(sourceRecordList.get(1).getOrder(), is(101));
  }

  @Test
  void shouldReturnSortedMarcAuthoritySourceRecordsOnGetWhenSortByOrderIsSpecified() {
    shouldReturnSortedMarcSourceRecordsOnGetWhenSortByOrderIsSpecified(RecordType.MARC_AUTHORITY, record_8,
      snapshot_4);
  }

  @Test
  void shouldReturnSortedMarcHoldingSourceRecordsOnGetWhenSortByOrderIsSpecified() {
    shouldReturnSortedMarcSourceRecordsOnGetWhenSortByOrderIsSpecified(RecordType.MARC_HOLDING, record_9,
      snapshot_5);
  }

  @Test
  void shouldReturnSortedEdifactSourceRecordsOnGetWhenSortByOrderIsSpecified() {
    postSnapshots(snapshot_2, snapshot_3);

    postRecords(record_2, record_3, record_5, record_6, record_7);

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

    assertThat(sourceRecordList.getFirst().getOrder(), is(0));
  }

  @Test
  void shouldReturnSourceRecordsForPeriod() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1);

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

    Date fromDate = new Date();
    String from = dateTimeFormatter.format(ZonedDateTime.ofInstant(fromDate.toInstant(), ZoneId.systemDefault()));

    // NOTE: record_5 saves but fails parsed record content validation and does not save parsed record
    postRecords(record_2, record_3, record_4, record_5);

    Date toDate = new Date();
    String to = dateTimeFormatter.format(ZonedDateTime.ofInstant(toDate.toInstant(), ZoneId.systemDefault()));

    postRecords(record_6);

    List<SourceRecord> sourceRecordList = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + from + "&updatedBefore=" + to)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(3))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .extract().response().body().as(SourceRecordCollection.class).getSourceRecords();

    // NOTE: we do not expect record_5 as they do not have a parsed record
    assertTrue(sourceRecordList.stream().map(SourceRecord::getRecordId).noneMatch(id -> id.equals(record_5.getId())));

    assertTrue(sourceRecordList.get(0).getMetadata().getUpdatedDate().after(fromDate));
    assertTrue(sourceRecordList.get(1).getMetadata().getUpdatedDate().after(fromDate));
    assertTrue(sourceRecordList.get(0).getMetadata().getUpdatedDate().before(toDate));
    assertTrue(sourceRecordList.get(1).getMetadata().getUpdatedDate().before(toDate));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + from)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(4))
      .body("sourceRecords*.deleted", everyItem(is(false)));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedAfter=" + to)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));

    // NOTE: we do not expect record_5 id does not have a parsed record
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedBefore=" + to)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(4))
      .body("totalRecords", is(4))
      .body("sourceRecords*.deleted", everyItem(is(false)));

    assertTrue(sourceRecordList.stream()
      .map(SourceRecord::getRecordId)
      .noneMatch(id -> id.equals(record_5.getId()) || id.equals(record_6.getId())));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?updatedBefore=" + from)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
  }

  @Test
  void shouldReturnSourceRecordsByListOfId() {
    postSnapshots(snapshot_1, snapshot_2);

    var firstSrsId = UUID.randomUUID().toString();
    var firstInstanceId = UUID.randomUUID().toString();
    var firstHrId = "hridFirst";

    var parsedRecord1 = new ParsedRecord().withId(firstSrsId)
      .withContent(new JsonObject().put("leader", "01542dcm a2200361   4500")
        .put("fields", new JsonArray().add(new JsonObject().put("999", new JsonObject()
          .put("subfields",
            new JsonArray().add(new JsonObject().put("s", firstSrsId)).add(new JsonObject().put("i", firstInstanceId)))
          .put("ind1", "f")
          .put("ind2", "f"))).add(new JsonObject().put("001", firstHrId))).encode());

    var deletedRecord1 = new Record()
      .withId(firstSrsId)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord1)
      .withMatchedId(firstSrsId)
      .withLeaderRecordStatus("d")
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(firstInstanceId)
        .withInstanceHrid(firstHrId));

    var secondSrsId = UUID.randomUUID().toString();
    var secondInstanceId = UUID.randomUUID().toString();
    var secondHrId = "hridSecond";

    var deletedRecord2 = new Record()
      .withId(secondSrsId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(SourceRecordApiTest.parsedRecord)
      .withMatchedId(secondSrsId)
      .withOrder(1)
      .withState(Record.State.DELETED)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(secondInstanceId)
        .withInstanceHrid(secondHrId));

    var records = new Record[] {record_1, record_2, record_3, record_4, record_6, deletedRecord1, deletedRecord2};
    postRecords(records);

    var ids = Arrays.stream(records)
      .map(Record::getId)
      .toList();

    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD&deleted=false")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(6))
      .body("totalRecords", is(6))
      .body("sourceRecords*.deleted", everyItem(is(false)));

    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD&deleted=true")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(7))
      .body("totalRecords", is(7));

    var externalIds = Arrays.stream(records)
      .map(rec -> rec.getExternalIdsHolder().getInstanceId())
      .toList();

    RestAssured.given()
      .spec(spec)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=INSTANCE&deleted=false")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(6))
      .body("totalRecords", is(6))
      .body("sourceRecords*.deleted", everyItem(is(false)));

    RestAssured.given()
      .spec(spec)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=INSTANCE&deleted=true")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(7))
      .body("totalRecords", is(7));

    RestAssured.given()
      .spec(spec)
      .body(ids)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?idType=RECORD")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(6))
      .body("totalRecords", is(6));
  }

  @Test
  void shouldReturnRecordsFromLocalAndCentralTenantsExcludeShadowRecordsByIdsListIfDeletedAndIncludeSharedParamsAreTrue() {
    Record shadowRecord = JsonObject.mapFrom(record_2).copy().mapTo(Record.class)
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_3.getJobExecutionId())
      .withState(Record.State.DELETED);

    postSnapshots(snapshot_1, snapshot_3);
    postSnapshots(CENTRAL_TENANT_ID, snapshot_2);
    postRecords(record_1, shadowRecord);
    postRecords(CENTRAL_TENANT_ID, record_2, record_6);

    List<String> externalIds = Stream.of(record_1, shadowRecord, record_2, record_6)
      .map(r -> r.getExternalIdsHolder().getInstanceId()).toList();

    RestAssured.given()
      .spec(spec)
      .queryParam("idType", IdType.INSTANCE)
      .queryParam("deleted", true)
      .queryParam("includeShared", true)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(3))
      .body("totalRecords", is(3))
      .body("sourceRecords*.externalIdsHolder.instanceId", hasItems(externalIds.toArray()))
      .body("sourceRecords*.snapshotId", hasItems(snapshot_1.getJobExecutionId(), snapshot_2.getJobExecutionId()))
      .body("sourceRecords*.snapshotId", not(hasItem(shadowRecord.getSnapshotId())));
  }

  @Test
  void shouldReturnRecordsOnlyFromLocalTenantByIdsListIfIncludeSharedIsFalseOrAbsent() {
    postSnapshots(snapshot_1);
    postSnapshots(CENTRAL_TENANT_ID, snapshot_2);
    postRecords(record_1, record_4);
    postRecords(CENTRAL_TENANT_ID, record_2);

    List<String> externalIds = Stream.of(record_1, record_4, record_2)
      .map(r -> r.getExternalIdsHolder().getInstanceId()).toList();
    List<String> expectedExternalIds = Stream.of(record_1, record_4)
      .map(r -> r.getExternalIdsHolder().getInstanceId()).toList();

    RestAssured.given()
      .spec(spec)
      .queryParam("idType", IdType.INSTANCE)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.externalIdsHolder.instanceId", hasItems(expectedExternalIds.toArray()))
      .body("sourceRecords*.externalIdsHolder.instanceId",
        not(hasItem(record_2.getExternalIdsHolder().getInstanceId())))
      .body("sourceRecords*.recordId", hasItems(record_1.getMatchedId(), record_4.getMatchedId()))
      .body("sourceRecords*.recordId", not(hasItem(record_2.getMatchedId())));
  }

  @Test
  void shouldReturnRecordOnlyFromLocalTenantByIdsListIfIncludeSharedIsTrueAndTenantIsNotInConsortium() {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_PATH), false))
      .willReturn(WireMock.ok().withBody(new JsonObject().put("userTenants", JsonArray.of()).encode())));

    postSnapshots(snapshot_1);
    postSnapshots(CENTRAL_TENANT_ID, snapshot_2);
    postRecords(record_1);
    postRecords(CENTRAL_TENANT_ID, record_2);

    List<String> externalIds = Stream.of(record_1, record_2)
      .map(r -> r.getExternalIdsHolder().getInstanceId()).toList();

    RestAssured.given()
      .spec(spec)
      .queryParam("idType", IdType.INSTANCE)
      .queryParam("includeShared", true)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1))
      .body("sourceRecords*.externalIdsHolder.instanceId", hasItem(record_1.getExternalIdsHolder().getInstanceId()))
      .body("sourceRecords*.externalIdsHolder.instanceId",
        not(hasItem(record_2.getExternalIdsHolder().getInstanceId())))
      .body("sourceRecords*.recordId", hasItem(record_1.getMatchedId()))
      .body("sourceRecords*.recordId", not(hasItem(record_2.getMatchedId())));
  }

  @Test
  void shouldReturnRecordsFromCentralTenantByIdsListIfCentralTenantSpecified() {
    postSnapshots(CENTRAL_TENANT_ID, snapshot_1);
    postRecords(CENTRAL_TENANT_ID, record_1, record_4);

    List<String> externalIds = Stream.of(record_1, record_4)
      .map(r -> r.getExternalIdsHolder().getInstanceId()).toList();

    RestAssured.given()
      .spec(spec)
      .header(XOkapiHeaders.TENANT, CENTRAL_TENANT_ID)
      .queryParam("idType", IdType.INSTANCE)
      .queryParam("includeShared", true)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.externalIdsHolder.instanceId", hasItems(externalIds.toArray()))
      .body("sourceRecords*.recordId", hasItems(record_1.getMatchedId(), record_4.getMatchedId()));
  }

  @Test
  void shouldReturnMarcAuthorityRecordsFromLocalAndCentralTenantsByIdsListIfIncludeSharedParamsIsTrue() {
    Record centralTenantRecord = new Record()
      .withId(FIRST_UUID)
      .withMatchedId(FIRST_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(RecordType.MARC_AUTHORITY)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withOrder(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityId(UUID.randomUUID().toString())
        .withAuthorityHrid("12345"));

    postSnapshots(snapshot_4);
    postSnapshots(CENTRAL_TENANT_ID, snapshot_1);
    postRecords(record_8);
    postRecords(CENTRAL_TENANT_ID, centralTenantRecord);

    List<String> externalIds = Stream.of(record_8, centralTenantRecord)
      .map(r -> r.getExternalIdsHolder().getAuthorityId()).toList();

    RestAssured.given()
      .spec(spec)
      .queryParam("recordType", RecordType.MARC_AUTHORITY)
      .queryParam("idType", IdType.AUTHORITY)
      .queryParam("includeShared", true)
      .body(externalIds)
      .when()
      .post(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2))
      .body("sourceRecords*.externalIdsHolder.authorityId", hasItems(externalIds.toArray()))
      .body("sourceRecords*.recordId", hasItems(record_8.getMatchedId(), centralTenantRecord.getMatchedId()));
  }

  @Test
  void shouldReturnEmptyListOnGetResultsIfNoRecordsExist() {
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
  void shouldReturnMarcBibParsedResultsOnGetWhenNoQueryIsSpecified() {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(snapshot_3, record_7, 4, RecordType.MARC_BIB);
  }

  @Test
  void shouldReturnMarcAuthorityParsedResultsOnGetWhenNoQueryIsSpecified() {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(snapshot_4, record_8, 1,
      RecordType.MARC_AUTHORITY);
  }

  @Test
  void shouldReturnMarcHoldingsParsedResultsOnGetWhenNoQueryIsSpecified() {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(snapshot_5, record_9, 1, RecordType.MARC_HOLDING);
  }

  @Test
  void shouldReturnEdifactParsedResultsOnGetWhenReturnTypeQueryIsSpecified() {
    shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(snapshot_3, record_7, 1, RecordType.EDIFACT);
  }

  @Test
  void shouldUnDeletedRecord() {
    postSnapshots(snapshot_1);
    var deletedRecord = new Record()
      .withId(FIRST_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(0)
      .withState(Record.State.DELETED)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_UUID));
    postRecords(deletedRecord);

    RestAssured.given()
      .spec(spec)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH + "/" + deletedRecord.getId() + "/un-delete")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + deletedRecord.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("deleted", is(false));
  }

  @Test
  void shouldReturnParsedResultsWithAnyStateWithNoParametersSpecified() {
    postSnapshots(snapshot_1, snapshot_2);

    Record recordWithOldState = new Record()
      .withId(SECOND_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(1)
      .withState(Record.State.OLD)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(FIRST_HRID));

    Record recordWithoutDeletedState = new Record()
      .withId(THIRD_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(0)
      .withState(Record.State.DELETED)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(THIRD_UUID).withInstanceHrid(FIRST_HRID));

    Record recordWithActualState = new Record()
      .withId(FOURTH_UUID)
      .withSnapshotId(snapshot_1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(1)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FOURTH_UUID).withInstanceHrid(FIRST_HRID));

    postRecords(recordWithOldState, recordWithoutDeletedState, recordWithActualState);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("sourceRecords*.parsedRecord", notNullValue());
  }

  @Test
  void shouldReturnResultsOnGetBySpecifiedSnapshotId() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1, record_2, record_3, record_4);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("sourceRecords*.snapshotId", everyItem(is(record_2.getSnapshotId())))
      .body("sourceRecords*.deleted", everyItem(is(false)))
      .body("sourceRecords*.additionalInfo.suppressDiscovery", everyItem(is(false)));
  }

  @Test
  void shouldReturnLimitedResultCollectionOnGetWithLimit() {
    postSnapshots(snapshot_1, snapshot_2);

    postRecords(record_1, record_2, record_3, record_4);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?limit=1")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", greaterThanOrEqualTo(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
  }

  @Test
  void shouldReturnAllSourceRecordsMarkedAsDeletedOnFindByRecordStateDeleted() {
    postSnapshots(snapshot_2);

    var createParsed = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);

    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));

    var parsedRecord1 = createParsed.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsedRecord1.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    var matchedId = UUID.randomUUID().toString();

    var record3 = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(SourceRecordApiTest.parsedRecord)
      .withMatchedId(matchedId)
      .withOrder(11)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(matchedId)
        .withInstanceHrid("12345"));

    createParsed = RestAssured.given()
      .spec(spec)
      .body(record3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
    parsedRecord1 = createParsed.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + parsedRecord1.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?deleted=true")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", greaterThanOrEqualTo(2))
      .body("sourceRecords*.deleted", everyItem(is(true)));
  }

  @Test
  void shouldReturnOnlyUnmarkedAsDeletedSourceRecordOnGetWhenParameterDeletedIsNotPassed() {
    postSnapshots(snapshot_2);

    postRecords(record_2);

    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record recordToDelete = createResponse.body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(SOURCE_STORAGE_RECORDS_PATH + "/" + recordToDelete.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", greaterThanOrEqualTo(1))
      .body("sourceRecords*.deleted", everyItem(is(false)));
  }

  @Test
  void shouldReturnBadRequestOnInvalidQueryParameters() {
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
  void shouldReturnSourceRecordWithAdditionalInfoOnGetBySpecifiedSnapshotId() {
    postSnapshots(snapshot_2);

    String matchedId = UUID.randomUUID().toString();

    Record newRecord = new Record()
      .withId(matchedId)
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(matchedId)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_HRID))
      .withAdditionalInfo(
        new AdditionalInfo().withSuppressDiscovery(true));

    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(newRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);

    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      // NOTE: we have to specify suppressFromDiscovery query parameter otherwise it will filter on the forced default of false
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?snapshotId=" + newRecord.getSnapshotId() + "&suppressFromDiscovery=true");
    assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    SourceRecordCollection sourceRecordCollection = getResponse.body().as(SourceRecordCollection.class);
    assertThat(sourceRecordCollection.getSourceRecords().size(), is(1));
    SourceRecord sourceRecord = sourceRecordCollection.getSourceRecords().getFirst();
    assertThat(sourceRecord.getRecordId(), is(createdRecord.getId()));
    assertThat(sourceRecord.getAdditionalInfo().getSuppressDiscovery(),
      is(createdRecord.getAdditionalInfo().getSuppressDiscovery()));
  }

  @Test
  void shouldReturnActualRecordsOnFilteringByDeleted() {
    postSnapshots(snapshot_2);

    Record record1 = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withLeaderRecordStatus("d")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(FIRST_UUID).withInstanceHrid(FIRST_HRID))
      .withState(Record.State.OLD);

    Record record2 = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withLeaderRecordStatus("d")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(SECOND_UUID).withInstanceHrid(SECOND_HRID))
      .withState(Record.State.ACTUAL);

    Record record3 = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot_2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(UUID.randomUUID().toString())
      .withLeaderRecordStatus("d")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(THIRD_UUID).withInstanceHrid(THIRD_HRID))
      .withState(Record.State.DELETED);

    postRecords(record1, record2, record3);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?deleted=true")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(2))
      .body("totalRecords", is(2));
  }

  private void shouldReturnSpecificMarcRecordSourceRecordOnGetByRecordId(RecordType recordType,
                                                                         Record aRecord, Snapshot snapshot) {
    postSnapshots(snapshot_1, snapshot_2, snapshot_3, snapshot);

    postRecords(record_1, record_3, record_7);

    Record createdRecord = RestAssured.given()
      .spec(spec)
      .body(aRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=" + recordType + "&recordId=" + createdRecord.getId()
        + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
  }

  private void returnSpecificMarcSourceRecordOnGetByDefaultExternalId(Snapshot snapshot,
                                                                      RecordType recordType) {
    postSnapshots(snapshot_1, snapshot);

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, SECOND_HRID);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, FIRST_HRID);

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
  }

  private void returnSpecificMarcSourceRecordOnGetByExternalId(Snapshot snapshot,
                                                               RecordType recordType) {
    postSnapshots(snapshot_1, snapshot);

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, SECOND_HRID);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, FIRST_HRID);

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
    String externalHrId = "hridExternal";

    Record recordWithOldState = new Record().withId(FOURTH_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD);
    setExternalIds(recordWithOldState, recordType, externalId, externalHrId);

    Record marcRecord = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(SourceRecordApiTest.parsedRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(marcRecord, recordType, externalId, externalHrId);

    RestAssured.given()
      .spec(spec)
      .body(marcRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(recordWithOldState)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

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
  }

  private String getIdType(RecordType recordType){
    if (Record.RecordType.MARC_BIB == recordType) {
      return "INSTANCE";
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return "HOLDINGS";
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      return "AUTHORITY";
    } else {
      return null;
    }
  }

  private void returnSpecificNumberOfMarcSourceRecordsOnGetByExternalHrid(Snapshot snapshot, RecordType recordType,
                                                                          String url) {
    postSnapshots(snapshot_1, snapshot);

    var firstHrid = "123";
    var secondHrid = "1234";
    var thirdHrid = "1235";

    var firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, firstHrid);

    var secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
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

    var recordWithOldState = new Record().withId(FOURTH_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FOURTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD);
    setExternalIds(recordWithOldState, recordType, THIRD_UUID, thirdHrid);

    var marcRecord = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(marcRecord, recordType, FOURTH_UUID, secondHrid);

    RestAssured.given()
      .spec(spec)
      .body(marcRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    RestAssured.given()
      .spec(spec)
      .body(recordWithOldState)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

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
  }

  private void setExternalIds(Record marcRecord, RecordType recordType, String id, String hrid) {
    if (recordType == RecordType.MARC_BIB) {
      marcRecord.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(id).withInstanceHrid(hrid));
    } else if (recordType == RecordType.MARC_HOLDING) {
      marcRecord.setExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(id).withHoldingsHrid(hrid));
    } else if (recordType == RecordType.MARC_AUTHORITY) {
      marcRecord.setExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(id));
    }
  }

  private void returnSpecificMarcSourceRecordOnGetByRecordExternalId(Snapshot snapshot,
                                                                     RecordType recordType) {
    postSnapshots(snapshot_1, snapshot);

    Record firstRecord = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(firstRecord, recordType, SECOND_UUID, SECOND_HRID);

    Record secondRecord = new Record().withId(SECOND_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(SECOND_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(secondRecord, recordType, FIRST_UUID, FIRST_HRID);

    Record recordWithOldState = new Record().withId(FIFTH_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIFTH_UUID)
      .withOrder(11)
      .withState(Record.State.OLD);
    setExternalIds(recordWithOldState, recordType, FIRST_UUID, FIRST_HRID);

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

    Record marcRecord = new Record().withId(THIRD_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(THIRD_UUID)
      .withOrder(11)
      .withState(Record.State.ACTUAL);
    setExternalIds(marcRecord, recordType, instanceId, "hridExternal");

    RestAssured.given()
      .spec(spec)
      .body(marcRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

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
  }

  private void returnSpecificMarcSourceRecordOnGetByRecordExternalIdAndRecordState(Snapshot snapshot,
                                                                                   RecordType recordType,
                                                                                   Record.State state) {
    postSnapshots(snapshot_1, snapshot);

    Record recordWithState = new Record().withId(FIRST_UUID)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withRecordType(recordType)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withMatchedId(FIRST_UUID)
      .withOrder(11)
      .withState(state);
    setExternalIds(recordWithState, recordType, SECOND_UUID, SECOND_HRID);

    RestAssured.given()
      .spec(spec)
      .body(recordWithState)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    var validatableResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "/" + FIRST_UUID + "?idType=RECORD&state=" + state)
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
        .body("externalIdsHolder.instanceId", nullValue())
        .body("externalIdsHolder.holdingsId", nullValue());
    }
  }

  private void shouldReturnSpecificMarcSourceRecordOnGetByRecordLeaderRecordStatus(RecordType recordType,
                                                                                   Record marcRecord,
                                                                                   Snapshot snapshot) {
    postSnapshots(snapshot_1, snapshot_2, snapshot);

    postRecords(record_1, record_3);

    var createdRecord = RestAssured.given()
      .spec(spec)
      .body(marcRecord)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .body().as(Record.class);

    var leaderStatus = ParsedRecordDaoUtil.getLeaderStatus(createdRecord.getParsedRecord());

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?recordType=" + recordType + "&leaderRecordStatus=" + leaderStatus
        + "&limit=1&offset=0")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("sourceRecords.size()", is(1))
      .body("totalRecords", is(1));
  }

  private void shouldReturnSortedMarcSourceRecordsOnGetWhenSortByOrderIsSpecified(RecordType recordType,
                                                                                  Record marcRecord,
                                                                                  Snapshot snapshot) {
    postSnapshots(snapshot_2, snapshot_3, snapshot);

    postRecords(record_2, record_3, record_5, record_6, record_7, marcRecord);

    // NOTE: get source records will not return if there is no associated parsed record
    var sourceRecordList = RestAssured.given()
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

    assertThat(sourceRecordList.getFirst().getOrder(), is(0));
  }

  private void shouldReturnMarcParsedResultsOnGetWhenNoQueryIsSpecified(Snapshot snapshot,
                                                                        Record marcRecord, int totalRecords,
                                                                        RecordType recordType) {
    postSnapshots(snapshot_1, snapshot_2, snapshot);

    postRecords(record_1, record_2, record_3, record_4, marcRecord);

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
  }

}
