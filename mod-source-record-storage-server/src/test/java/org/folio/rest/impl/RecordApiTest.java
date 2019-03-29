package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@RunWith(VertxUnitRunner.class)
public class RecordApiTest extends AbstractRestVerticleTest {

  static final String SOURCE_STORAGE_SOURCE_RECORDS_PATH = "/source-storage/sourceRecords";
  private static final String SOURCE_STORAGE_RECORDS_PATH = "/source-storage/records";
  private static final String SOURCE_STORAGE_SNAPSHOTS_PATH = "/source-storage/snapshots";
  private static final String SNAPSHOTS_TABLE_NAME = "snapshots";
  private static final String RECORDS_TABLE_NAME = "records";
  private static final String RAW_RECORDS_TABLE_NAME = "raw_records";
  private static final String ERROR_RECORDS_TABLE_NAME = "error_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";

  private static RawRecord rawRecord_1 = new RawRecord()
    .withContent("01542ccm a2200361   450000100070000000500170000700800410002401000190006503500200008403500110010404" +
      "0002300115041001900138045000900157047007500166050001400241100004200255240001000297245016700307246002400474260" +
      "0036004983000025005345050314005596500016008736500023008896500049009126500042009619020023010039050021010269480" +
      "04201047948002701089948002701116948003701143\u001e393893\u001e20141107001016.0\u001e830419m19559999gw mua   hiz" +
      "   n    lat  \u001e  \u001fa   55001156/M \u001e  \u001fa(OCoLC)63611770\u001e  \u001fa393893\u001e  \u001fcUPB" +
      "\u001fdUPB\u001fdNIC\u001fdNIC\u001e0 \u001falatitager\u001fgger\u001e  \u001fav6v9\u001e  \u001facn\u001fact" +
      "\u001faco\u001fadf\u001fadv\u001faft\u001fafg\u001fams\u001fami\u001fanc\u001faop\u001faov\u001farq\u001fasn" +
      "\u001fasu\u001fasy\u001favr\u001fazz\u001e0 \u001faM3\u001fb.M896\u001e1 \u001faMozart, Wolfgang Amadeus,\u001f" +
      "d1756-1791.\u001e10\u001faWorks\u001e10\u001faNeue Ausgabe sämtlicher Werke,\u001fbin Verbindung mit den Mozartstädten," +
      " Augsburg, Salzburg und Wien.\u001fcHrsg. von der Internationalen Stiftung Mozarteum, Salzburg.\u001e33\u001f" +
      "aNeue Mozart-Ausgabe\u001e  \u001faKassel,\u001fbBärenreiter,\u001fcc1955-\u001e  \u001fav.\u001fbfacsims.\u001fc33 cm." +
      "\u001e0 \u001faSer. I. Geistliche Gesangswerke -- Ser. II. Opern -- Ser. III. Lieder, mehrstimmige Gesänge, Kanons" +
      " -- Ser. IV. Orchesterwerke -- Ser. V. Konzerte -- Ser. VI. Kirchensonaten -- Ser. VII. Ensemblemusik für grössere " +
      "Solobesetzungen -- Ser. VIII. Kammermusik -- Ser. IX. Klaviermusik -- Ser. X. Supplement.\u001e 0\u001faVocal music" +
      "\u001e 0\u001faInstrumental music\u001e 7\u001faInstrumental music\u001f2fast\u001f0(OCoLC)fst00974414\u001e 7\u001f" +
      "aVocal music\u001f2fast\u001f0(OCoLC)fst01168379\u001e  \u001fapfnd\u001fbAustin Music\u001e  \u001fa19980728120000.0" +
      "\u001e1 \u001fa20100622\u001fbs\u001fdlap11\u001felts\u001fxToAddCatStat\u001e0 \u001fa20110818\u001fbr\u001fdnp55" +
      "\u001felts\u001e2 \u001fa20130128\u001fbm\u001fdbmt1\u001felts\u001e2 \u001fa20141106\u001fbm\u001fdbatch\u001felts\u001fxaddfast\u001e\u001d");
  private static ParsedRecord marcRecord = new ParsedRecord()
    .withContent("{\"leader\":\"01542ccm a2200361   4500\",\"fields\":[{\"001\":\"393893\"},{\"005\":\"20141107001016.0\"}," +
      "{\"008\":\"830419m19559999gw mua   hiz   n    lat  \"},{\"010\":{\"subfields\":[{\"a\":\"   55001156/M \"}],\"ind1\":\"" +
      " \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)63611770\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\"" +
      ":[{\"a\":\"393893\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"040\":{\"subfields\":[{\"c\":\"UPB\"},{\"d\":\"UPB\"},{\"d\":\"NIC\"}," +
      "{\"d\":\"NIC\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"041\":{\"subfields\":[{\"a\":\"latitager\"},{\"g\":\"ger\"}],\"ind1\":\"0\"," +
      "\"ind2\":\" \"}},{\"045\":{\"subfields\":[{\"a\":\"v6v9\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"047\":{\"subfields\":[{\"a\":\"cn\"}," +
      "{\"a\":\"ct\"},{\"a\":\"co\"},{\"a\":\"df\"},{\"a\":\"dv\"},{\"a\":\"ft\"},{\"a\":\"fg\"},{\"a\":\"ms\"},{\"a\":\"mi\"},{\"a\":\"nc\"}," +
      "{\"a\":\"op\"},{\"a\":\"ov\"},{\"a\":\"rq\"},{\"a\":\"sn\"},{\"a\":\"su\"},{\"a\":\"sy\"},{\"a\":\"vr\"},{\"a\":\"zz\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"050\":{\"subfields\":[{\"a\":\"M3\"},{\"b\":\".M896\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"100\":{\"subfields\":[{\"a\":\"Mozart, Wolfgang Amadeus,\"}," +
      "{\"d\":\"1756-1791.\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"240\":{\"subfields\":[{\"a\":\"Works\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"245\":{\"subfields\"" +
      ":[{\"a\":\"Neue Ausgabe sa\\u0308mtlicher Werke,\"},{\"b\":\"in Verbindung mit den Mozartsta\\u0308dten, Augsburg, Salzburg und Wien.\"},{\"c\":\"Hrsg. von der" +
      " Internationalen Stiftung Mozarteum, Salzburg.\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"246\":{\"subfields\":[{\"a\":\"Neue Mozart-Ausgabe\"}],\"ind1\":\"3\"," +
      "\"ind2\":\"3\"}},{\"260\":{\"subfields\":[{\"a\":\"Kassel,\"},{\"b\":\"Ba\\u0308renreiter,\"},{\"c\":\"c1955-\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"300\":" +
      "{\"subfields\":[{\"a\":\"v.\"},{\"b\":\"facsims.\"},{\"c\":\"33 cm.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"505\":{\"subfields\":[{\"a\":\"Ser. I. Geistliche" +
      " Gesangswerke -- Ser. II. Opern -- Ser. III. Lieder, mehrstimmige Gesa\\u0308nge, Kanons -- Ser. IV. Orchesterwerke -- Ser. V. Konzerte -- Ser. VI. Kirchensonaten " +
      "-- Ser. VII. Ensemblemusik fu\\u0308r gro\\u0308ssere Solobesetzungen -- Ser. VIII. Kammermusik -- Ser. IX. Klaviermusik -- Ser. X. Supplement.\"}],\"ind1\":\"0\"," +
      "\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Vocal music\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Instrumental music\"}],\"ind1\":" +
      "\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Instrumental music\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst00974414\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"650\"" +
      ":{\"subfields\":[{\"a\":\"Vocal music\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst01168379\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"902\":{\"subfields\":[{\"a\":\"pfnd\"},{\"b\"" +
      ":\"Austin Music\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"905\":{\"subfields\":[{\"a\":\"19980728120000.0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"948\":{\"subfields\":[{\"a\"" +
      ":\"20100622\"},{\"b\":\"s\"},{\"d\":\"lap11\"},{\"e\":\"lts\"},{\"x\":\"ToAddCatStat\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"948\":{\"subfields\":[{\"a\":\"20110818\"}," +
      "{\"b\":\"r\"},{\"d\":\"np55\"},{\"e\":\"lts\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"948\":{\"subfields\":[{\"a\":\"20130128\"},{\"b\":\"m\"},{\"d\":\"bmt1\"},{\"e\":\"lts\"}]," +
      "\"ind1\":\"2\",\"ind2\":\" \"}},{\"948\":{\"subfields\":[{\"a\":\"20141106\"},{\"b\":\"m\"},{\"d\":\"batch\"},{\"e\":\"lts\"},{\"x\":\"addfast\"}],\"ind1\":\"2\",\"ind2\":\"" +
      " \"}}]}\n");
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
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord_1)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_2 = new Record()
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord_1)
    .withParsedRecord(marcRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_3 = new Record()
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord_1)
    .withErrorRecord(errorRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_4 = new Record()
    .withSnapshotId(snapshot_1.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord_1)
    .withParsedRecord(marcRecord)
    .withMatchedId(UUID.randomUUID().toString());
  private static Record record_5 = new Record()
    .withSnapshotId(snapshot_2.getJobExecutionId())
    .withRecordType(Record.RecordType.MARC)
    .withRawRecord(rawRecord_1)
    .withMatchedId(UUID.randomUUID().toString())
    .withParsedRecord(invalidParsedRecord);
  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(RECORDS_TABLE_NAME, new Criterion(), event -> {
      pgClient.delete(RAW_RECORDS_TABLE_NAME, new Criterion(), event1 -> {
        pgClient.delete(ERROR_RECORDS_TABLE_NAME, new Criterion(), event2 -> {
          pgClient.delete(MARC_RECORDS_TABLE_NAME, new Criterion(), event3 -> {
            pgClient.delete(SNAPSHOTS_TABLE_NAME, new Criterion(), event4 -> {
              if (event4.failed()) {
                context.fail(event4.cause());
              }
              async.complete();
            });
          });
        });
      });
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
  public void shouldReturnAllRecordsOnGetWhenNoQueryIsSpecified(TestContext testContext) {
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
      .get(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(recordsToPost.size()));
    async.complete();
  }

  @Test
  public void shouldReturnRecordsOnGetBySpecifiedSnapshotId(TestContext testContext) {
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
      .get(SOURCE_STORAGE_RECORDS_PATH + "?query=snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(2))
      .body("records*.snapshotId", everyItem(is(record_2.getSnapshotId())));
    async.complete();
  }

  @Test
  public void shouldReturnLimitedCollectionOnGetWithLimit(TestContext testContext) {
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
      .get(SOURCE_STORAGE_RECORDS_PATH + "?limit=2")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("records.size()", is(2))
      .body("totalRecords", is(recordsToPost.size()));
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
  public void shouldCreateRecordOnPost(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_1.getSnapshotId()))
      .body("recordType", is(record_1.getRecordType().name()))
      .body("rawRecord.content", is(rawRecord_1.getContent()));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordOnPost(TestContext testContext) {
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
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .body("snapshotId", is(record_3.getSnapshotId()))
      .body("recordType", is(record_3.getRecordType().name()))
      .body("rawRecord.content", is(rawRecord_1.getContent()))
      .body("errorRecord.content", is(errorRecord.getContent()));
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
  public void shouldUpdateExistingRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    createdRecord.setParsedRecord(marcRecord);
    Response putResponse = RestAssured.given()
      .spec(spec)
      .body(createdRecord)
      .when()
      .put(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    Assert.assertThat(putResponse.statusCode(), is(HttpStatus.SC_OK));
    Record updatedRecord = putResponse.body().as(Record.class);
    Assert.assertThat(updatedRecord.getId(), is(createdRecord.getId()));
    Assert.assertThat(updatedRecord.getRawRecord().getContent(), is(rawRecord_1.getContent()));
    ParsedRecord parsedRecord = updatedRecord.getParsedRecord();
    Assert.assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
    async.complete();
  }

  @Test
  public void shouldUpdateErrorRecordOnPut(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(snapshot_1)
      .when()
      .post(SOURCE_STORAGE_SNAPSHOTS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED);
    async.complete();

    async = testContext.async();
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_1)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
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
      .body("errorRecord.content", is(createdRecord.getErrorRecord().getContent()));
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
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_2)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    Assert.assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    Assert.assertThat(getRecord.getId(), is(createdRecord.getId()));
    Assert.assertThat(getRecord.getRawRecord().getContent(), is(rawRecord_1.getContent()));
    ParsedRecord parsedRecord = getRecord.getParsedRecord();
    Assert.assertThat(JsonObject.mapFrom(parsedRecord.getContent()).encode(), containsString("\"leader\":\"01542ccm a2200361   4500\""));
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
  public void shouldDeleteExistingRecordOnDelete(TestContext testContext) {
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
    Assert.assertThat(createParsed.statusCode(), is(HttpStatus.SC_CREATED));
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
    Response createErrorRecord = RestAssured.given()
      .spec(spec)
      .body(record_3)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    Assert.assertThat(createErrorRecord.statusCode(), is(HttpStatus.SC_CREATED));
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
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=snapshotId=" + record_2.getSnapshotId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("sourceRecords*.snapshotId", everyItem(is(record_2.getSnapshotId())));
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
      .body("totalRecords", greaterThanOrEqualTo(1));
    async.complete();
  }

  @Test
  public void shouldCreateErrorRecordIfParsedContentIsInvalid(TestContext testContext) {
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
    Response createResponse = RestAssured.given()
      .spec(spec)
      .body(record_5)
      .when()
      .post(SOURCE_STORAGE_RECORDS_PATH);
    Assert.assertThat(createResponse.statusCode(), is(HttpStatus.SC_CREATED));
    Record createdRecord = createResponse.body().as(Record.class);
    async.complete();

    async = testContext.async();
    Response getResponse = RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "/" + createdRecord.getId());
    Assert.assertThat(getResponse.statusCode(), is(HttpStatus.SC_OK));
    Record getRecord = getResponse.body().as(Record.class);
    Assert.assertThat(getRecord.getId(), is(createdRecord.getId()));
    Assert.assertThat(getRecord.getRawRecord().getContent(), is(rawRecord_1.getContent()));
    Assert.assertThat(getRecord.getParsedRecord(), nullValue());
    Assert.assertThat(getRecord.getErrorRecord(), notNullValue());
    async.complete();
  }

  @Test
  public void shouldReturnErrorOnGet() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=error!")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?query=select * from table")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?query=error!")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?query=select * from table")
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_RECORDS_PATH + "?limit=select * from table")
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }
}
