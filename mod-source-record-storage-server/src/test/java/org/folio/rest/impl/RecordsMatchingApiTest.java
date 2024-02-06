package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Filter;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordMatchingDto;
import org.folio.rest.jaxrs.model.Snapshot;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

@RunWith(VertxUnitRunner.class)
public class RecordsMatchingApiTest extends AbstractRestVerticleTest {

  private static final String RECORDS_MATCHING_PATH = "/source-storage/records/matching";
  private static final String PARSED_MARC_BIB_WITH_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/marcBibContentWith999field.json";
  private static final String PARSED_MARC_AUTHORITY_WITH_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/parsedMarcAuthorityWith999field.json";
  private static final String PARSED_MARC_HOLDINGS_WITH_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/marcHoldingsContentWith999field.json";
  private static final String PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH = "src/test/resources/parsedMarcRecordContent.sample";

  private static String rawRecordContent;
  private static String parsedRecordContent;

  private Snapshot snapshot;
  private Record existingRecord;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    rawRecordContent = TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH);
    parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_BIB_WITH_999_FIELD_SAMPLE_PATH);
  }

  @Before
  public void setUp(TestContext context) {
    snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    String existingRecordId = "acf4f6e2-115c-4509-9d4c-536c758ef917";
    this.existingRecord = new Record()
      .withId(existingRecordId)
      .withMatchedId(existingRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(new RawRecord().withId(existingRecordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(existingRecordId).withContent(parsedRecordContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("681394b4-10d8-4cb1-a618-0f9bd6152119").withInstanceHrid("12345"));

    postSnapshots(context, snapshot);
    postRecords(context, existingRecord);
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID))
      .onSuccess(v -> async.complete())
      .onFailure(context::fail);
  }

  @Test
  public void shouldReturnEmptyCollectionIfRecordsDoNotMatch() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of(UUID.randomUUID().toString()))
          .withField("999")
          .withIndicator1("f")
          .withIndicator2("f")
          .withSubfield("s"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("identifiers.size()", is(0));
  }

  @Test
  public void shouldMatchRecordByMatchedIdField() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
        .withValues(List.of(existingRecord.getMatchedId()))
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("s"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchMarcBibRecordByInstanceIdField() {
    shouldMatchRecordByExternalIdField(existingRecord);
  }

  @Test
  public void shouldMatchMarcAuthorityRecordByAuthorityIdField(TestContext context) throws IOException {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_AUTHORITY_WITH_999_FIELD_SAMPLE_PATH);
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(UUID.randomUUID().toString()));

    postRecords(context, record);
    shouldMatchRecordByExternalIdField(record);
  }

  @Test
  public void shouldMatchMarcHoldingsRecordByHoldingIdField(TestContext context) throws IOException {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_HOLDINGS_WITH_999_FIELD_SAMPLE_PATH);
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_HOLDING)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(UUID.randomUUID().toString()));

    postRecords(context, record);
    shouldMatchRecordByExternalIdField(record);
  }

  private void shouldMatchRecordByExternalIdField(Record sourceRecord) {
    String externalId = RecordDaoUtil.getExternalId(sourceRecord.getExternalIdsHolder(), sourceRecord.getRecordType());
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.valueOf(sourceRecord.getRecordType().name()))
        .withFilters(List.of(new Filter()
          .withValues(List.of(externalId))
          .withField("999")
          .withIndicator1("f")
          .withIndicator2("f")
          .withSubfield("i"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(sourceRecord.getId()))
      .body("identifiers[0].externalId", is(externalId));
  }

  @Test
  public void shouldMatchRecordByInstanceHridField() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of(existingRecord.getExternalIdsHolder().getInstanceHrid()))
          .withField("001"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchRecordByMultipleDataFields() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12345", "oclc1234567"))
          .withField("035")
          .withIndicator1("")
          .withIndicator2("")
          .withSubfield("a"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchRecordByMultipleControlledFields() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12569", "364345"))
          .withField("007"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchRecordByMultiple024FieldsWithWildcardsInd() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12345", "test123"))
          .withField("024")
          .withIndicator1("*")
          .withIndicator2("*")
          .withSubfield("a"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldNotMatchRecordBy035FieldIfRecordExternalIdIsNull(TestContext context) throws IOException {
    String parsedRecordContent = new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH), JsonObject.class).encode();
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent));

    postRecords(context, record);

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("(OCoLC)63611770", "1234567"))
          .withField("035")
          .withIndicator1("")
          .withIndicator2("")
          .withSubfield("a"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("identifiers.size()", is(0));
  }

  @Test
  public void shouldReturnLimitedRecordsIdentifiersCollectionWithLimitAndOffset(TestContext context) throws IOException {
    String parsedRecordContent = new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH), JsonObject.class).encode();
    List<String> recordsIds = List.of("00000000-0000-1000-8000-000000000004", "00000000-0000-1000-8000-000000000002",
      "00000000-0000-1000-8000-000000000003", "00000000-0000-1000-8000-000000000001");

    for (String recordId : recordsIds) {
      Record record = new Record()
        .withId(recordId)
        .withMatchedId(recordId)
        .withSnapshotId(snapshot.getJobExecutionId())
        .withGeneration(0)
        .withRecordType(MARC_BIB)
        .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
        .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent))
        .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()));

      postRecords(context, record);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withLimit(2)
        .withOffset(2)
        .withFilters(List.of(new Filter()
          .withValues(List.of("(OCoLC)63611770", "1234567"))
          .withField("035")
          .withIndicator1("")
          .withIndicator2("")
          .withSubfield("a"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(4))
      .body("identifiers.size()", is(2))
      .body("identifiers.recordId",
        everyItem(is(oneOf("00000000-0000-1000-8000-000000000003", "00000000-0000-1000-8000-000000000004"))));
  }

  @Test
  public void shouldReturnUnprocessableEntityIfFilterIsNotSpecified() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of()))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void shouldReturnUnprocessableEntityIfValuesIsNotSpecified() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withField("035")
          .withIndicator1("")
          .withIndicator2("")
          .withSubfield("a"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

}
