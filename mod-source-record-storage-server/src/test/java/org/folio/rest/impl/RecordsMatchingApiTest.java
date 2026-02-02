package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.response.Response;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.MatchField;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Filter;
import org.folio.rest.jaxrs.model.Filter.ComparisonPartType;
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

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.folio.rest.jaxrs.model.Filter.ComparisonPartType.ALPHANUMERICS_ONLY;
import static org.folio.rest.jaxrs.model.Filter.ComparisonPartType.NUMERICS_ONLY;
import static org.folio.rest.jaxrs.model.Filter.Qualifier.BEGINS_WITH;
import static org.folio.rest.jaxrs.model.Filter.Qualifier.CONTAINS;
import static org.folio.rest.jaxrs.model.Filter.Qualifier.ENDS_WITH;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.rest.jaxrs.model.RecordMatchingDto.LogicalOperator.AND;
import static org.folio.rest.jaxrs.model.RecordMatchingDto.LogicalOperator.OR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

@RunWith(VertxUnitRunner.class)
public class RecordsMatchingApiTest extends AbstractRestVerticleTest {

  private static final String RECORDS_MATCHING_PATH = "/source-storage/records/matching";
  private static final String PARSED_MARC_BIB_WITH_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/marcBibContentWith999field.json";
  private static final String PARSED_MARC_BIB_WITHOUT_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/marcBibContentWithout999field.json";
  private static final String PARSED_MARC_AUTHORITY_WITH_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/parsedMarcAuthorityWith999field.json";
  private static final String PARSED_MARC_HOLDINGS_WITH_999_FIELD_SAMPLE_PATH = "src/test/resources/mock/parsedContents/marcHoldingsContentWith999field.json";
  private static final String PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH = "src/test/resources/parsedMarcRecordContent.sample";
  private static final String FIELD_007 = "12569";
  private static final String FIELD_035 = "12345";
  private static final int SPLIT_INDEX = 2;
  private static final String HR_ID = "hrId12345";

  private static String rawRecordContent;
  private static String parsedRecordContent;
  private static String parsedRecordContentWithout999;

  private Snapshot snapshot;
  private Record existingRecord;
  private Record existingDeletedRecord;

  @BeforeClass
  public static void setUpBeforeClass() {
    rawRecordContent = TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH);
    parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_BIB_WITH_999_FIELD_SAMPLE_PATH);
    parsedRecordContentWithout999 = TestUtil.readFileFromPath(PARSED_MARC_BIB_WITHOUT_999_FIELD_SAMPLE_PATH);
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

    String deletedRecordId = UUID.randomUUID().toString();
    this.existingDeletedRecord = new Record()
      .withId(deletedRecordId)
      .withMatchedId(deletedRecordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withState(Record.State.DELETED)
      .withRawRecord(new RawRecord().withId(deletedRecordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(deletedRecordId).withContent(parsedRecordContentWithout999))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid("54321"));

    postSnapshots(context, snapshot);
    postRecords(context, existingRecord, existingDeletedRecord);
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(PostgresClientFactory.getCachedPool(vertx, TENANT_ID))
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
          .withSubfield("s")
          .withQualifier(BEGINS_WITH)
          .withQualifierValue("TEST"))))
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
            .withQualifier(BEGINS_WITH)
            .withQualifierValue("acf")
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
  public void shouldMatchMarcBibRecordByInstanceIdFieldAndQualifier() {
    var instanceId = existingRecord.getExternalIdsHolder().getInstanceId();
    var beginWith = new MatchField.QualifierMatch(BEGINS_WITH, instanceId.substring(0, SPLIT_INDEX));
    var endWith =  new MatchField.QualifierMatch(ENDS_WITH, instanceId.substring(SPLIT_INDEX));
    var contains = new MatchField.QualifierMatch(CONTAINS, instanceId.substring(SPLIT_INDEX, SPLIT_INDEX + SPLIT_INDEX));
    shouldMatchRecordByExternalIdField(existingRecord, beginWith);
    shouldMatchRecordByExternalIdField(existingRecord, endWith);
    shouldMatchRecordByExternalIdField(existingRecord, contains);
  }

  @Test
  public void shouldMatchMarcAuthorityRecordByAuthorityIdField(TestContext context) {
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
  public void shouldMatchMarcHoldingsRecordByHoldingIdField(TestContext context) {
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

  private void shouldMatchRecordByExternalIdField(Record sourceRecord, MatchField.QualifierMatch qualifier) {
    var externalId = RecordDaoUtil.getExternalId(sourceRecord.getExternalIdsHolder(), sourceRecord.getRecordType());
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
          .withSubfield("i")
          .withQualifier(qualifier.qualifier())
          .withQualifierValue(qualifier.value()))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(sourceRecord.getId()))
      .body("identifiers[0].externalId", is(externalId));
  }

  @Test
  public void shouldMatchMarcBibRecordByInstanceIdFieldWithQualifierAndComparePart(TestContext context) {

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH);
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID));

    postRecords(context, record);

    var incomingValueNumeric = "ocn393893";
    var beginWith = new MatchField.QualifierMatch(BEGINS_WITH, incomingValueNumeric.substring(0, SPLIT_INDEX));
    var endWith =  new MatchField.QualifierMatch(ENDS_WITH, incomingValueNumeric.substring(SPLIT_INDEX));
    var contains = new MatchField.QualifierMatch(CONTAINS, incomingValueNumeric.substring(SPLIT_INDEX, SPLIT_INDEX + SPLIT_INDEX));

    shouldMatchRecordByExternalIdField(record, beginWith, NUMERICS_ONLY, "Case 1");
    shouldMatchRecordByExternalIdField(record, endWith, NUMERICS_ONLY, "Case 2");
    shouldMatchRecordByExternalIdField(record, contains, NUMERICS_ONLY, "Case 3");

    var incomingValueAlphaNumeric = "(OCoLC)63611770";
    var beginWithAlphaNumeric = new MatchField.QualifierMatch(BEGINS_WITH, incomingValueAlphaNumeric.substring(0, SPLIT_INDEX));
    var endWithAlphaNumeric =  new MatchField.QualifierMatch(ENDS_WITH, incomingValueAlphaNumeric.substring(SPLIT_INDEX));
    var containsAlphaNumeric = new MatchField.QualifierMatch(CONTAINS, incomingValueAlphaNumeric.substring(SPLIT_INDEX, SPLIT_INDEX + SPLIT_INDEX));

    shouldMatchRecordByExternalIdField(record, beginWithAlphaNumeric, ALPHANUMERICS_ONLY, "Case 4");
    shouldMatchRecordByExternalIdField(record, endWithAlphaNumeric, ALPHANUMERICS_ONLY, "Case 5");
    shouldMatchRecordByExternalIdField(record, containsAlphaNumeric, ALPHANUMERICS_ONLY, "Case 6");
  }

  private void shouldMatchRecordByExternalIdField(Record sourceRecord, MatchField.QualifierMatch qualifier,
                                                  ComparisonPartType comparisonPartType, String testName) {
    var externalId = RecordDaoUtil.getExternalId(sourceRecord.getExternalIdsHolder(), sourceRecord.getRecordType());
    Response response = RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.valueOf(sourceRecord.getRecordType().name()))
        .withFilters(List.of(new Filter()
          .withValues(List.of("OCoLC63611770", "393893"))
          .withField("035")
          .withIndicator1("")
          .withIndicator2("")
          .withSubfield("a")
          .withQualifier(qualifier.qualifier())
          .withQualifierValue(qualifier.value())
          .withComparisonPartType(comparisonPartType))))
      .post(RECORDS_MATCHING_PATH);

    response.then()
      .statusCode(HttpStatus.SC_OK);

    assertThat(testName, response.jsonPath().getInt("totalRecords"), is(1));
    assertThat(testName, response.jsonPath().getInt("identifiers.size()"), is(1));
    assertThat(testName, response.jsonPath().getString("identifiers[0].recordId"), is(sourceRecord.getId()));
    assertThat(testName, response.jsonPath().getString("identifiers[0].externalId"), is(externalId));
  }

  private void shouldMatchRecordByInstanceHridFieldAndQualifier(MatchField.QualifierMatch qualifier) {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of(existingRecord.getExternalIdsHolder().getInstanceHrid()))
          .withField("001")
          .withQualifier(qualifier.qualifier())
          .withQualifierValue(qualifier.value()))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
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
  public void shouldMatchDeletedRecordByInstanceHridField() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of(existingDeletedRecord.getExternalIdsHolder().getInstanceHrid()))
          .withField("001"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingDeletedRecord.getId()))
      .body("identifiers[0].externalId", is(existingDeletedRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchDeletedRecordByMatchedId() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of(existingDeletedRecord.getMatchedId()))
          .withField("999")
          .withIndicator1("f")
          .withIndicator2("f")
          .withSubfield("s"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingDeletedRecord.getId()))
      .body("identifiers[0].externalId", is(existingDeletedRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchRecordByInstanceHridFieldAndQualifier() {
    var hrId = existingRecord.getExternalIdsHolder().getInstanceHrid();
    var beginWith = new MatchField.QualifierMatch(BEGINS_WITH, hrId.substring(0, SPLIT_INDEX));
    var endWith =  new MatchField.QualifierMatch(ENDS_WITH, hrId.substring(SPLIT_INDEX));
    var contains = new MatchField.QualifierMatch(CONTAINS, hrId.substring(SPLIT_INDEX, SPLIT_INDEX + SPLIT_INDEX));
    shouldMatchRecordByInstanceHridFieldAndQualifier(beginWith);
    shouldMatchRecordByInstanceHridFieldAndQualifier(endWith);
    shouldMatchRecordByInstanceHridFieldAndQualifier(contains);
  }

  @Test
  public void shouldNotMatchMarcBibRecordByInstanceIdFieldAndQualifier(){
    var externalId = RecordDaoUtil.getExternalId(existingRecord.getExternalIdsHolder(), existingRecord.getRecordType());
    var qualifier = new MatchField.QualifierMatch(CONTAINS, "ABC");
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.valueOf(existingRecord.getRecordType().name()))
        .withFilters(List.of(new Filter()
          .withValues(List.of(externalId))
          .withField("999")
          .withIndicator1("f")
          .withIndicator2("f")
          .withSubfield("i")
          .withQualifier(qualifier.qualifier())
          .withQualifierValue(qualifier.value()))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("identifiers.size()", is(0));
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

  public void shouldMatchRecordByMultipleDataFieldsAndQualifier(MatchField.QualifierMatch qualifier) {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12345", "oclc1234567"))
          .withQualifier(qualifier.qualifier())
          .withQualifierValue(qualifier.value())
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
  public void shouldMatchRecordByMultipleDataFieldsAndQualifier() {
    var beginWith = new MatchField.QualifierMatch(BEGINS_WITH, FIELD_035.substring(0, SPLIT_INDEX));
    var endWith = new MatchField.QualifierMatch(ENDS_WITH, FIELD_035.substring(SPLIT_INDEX));
    var contains = new MatchField.QualifierMatch(CONTAINS, FIELD_035.substring(SPLIT_INDEX, SPLIT_INDEX + SPLIT_INDEX));
    shouldMatchRecordByMultipleDataFieldsAndQualifier(beginWith);
    shouldMatchRecordByMultipleDataFieldsAndQualifier(endWith);
    shouldMatchRecordByMultipleDataFieldsAndQualifier(contains);
  }

  @Test
  public void shouldNotMatchRecordByMultipleDataFieldsAndQualifier() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12345", "oclc1234567"))
          .withQualifier(BEGINS_WITH)
          .withQualifierValue("ABC")
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

  private void shouldMatchRecordByMultipleControlledFieldsAndQualifier(MatchField.QualifierMatch qualifier){
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12569", "364345"))
          .withQualifier(qualifier.qualifier())
          .withQualifierValue(qualifier.value())
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
  public void shouldMatchRecordByMultipleControlledFieldsAndQualifier() {
    var beginWith = new MatchField.QualifierMatch(BEGINS_WITH, FIELD_007.substring(0, SPLIT_INDEX));
    var endWith = new MatchField.QualifierMatch(ENDS_WITH, FIELD_007.substring(SPLIT_INDEX));
    var contains = new MatchField.QualifierMatch(CONTAINS, FIELD_007.substring(SPLIT_INDEX, SPLIT_INDEX + SPLIT_INDEX));
    shouldMatchRecordByMultipleControlledFieldsAndQualifier(beginWith);
    shouldMatchRecordByMultipleControlledFieldsAndQualifier(endWith);
    shouldMatchRecordByMultipleControlledFieldsAndQualifier(contains);
  }

  @Test
  public void shouldNotMatchRecordByMultipleControlledFieldsAndQualifier(){
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("12569", "364345"))
          .withQualifier(BEGINS_WITH)
          .withQualifierValue("ABC")
          .withField("007"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .body("identifiers.size()", is(0));
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
  public void shouldNotMatchRecordBy035FieldIfRecordExternalIdIsNull(TestContext context) {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_AUTHORITY_WITH_999_FIELD_SAMPLE_PATH);
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent));

    postRecords(context, record);

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_AUTHORITY)
        .withFilters(List.of(new Filter()
          .withValues(List.of("(OCoLC)63611770", "nin00009530412"))
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
  public void shouldReturnLimitedRecordsIdentifiersCollectionWithLimitAndOffset(TestContext context) {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH);
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
        .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID));

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
  public void shouldMatchRecordByMarcFieldAndExternalIds(TestContext context) {
    String parsedContent = TestUtil.readFileFromPath(PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH);
    Record[] records = IntStream.range(0, 2)
      .mapToObj(i -> UUID.randomUUID().toString())
      .map(recordId -> new Record()
        .withId(recordId)
        .withMatchedId(recordId)
        .withSnapshotId(snapshot.getJobExecutionId())
        .withGeneration(0)
        .withRecordType(MARC_BIB)
        .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
        .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedContent))
        .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString())
          .withInstanceHrid(String.valueOf(new Random().nextInt(100)))))
      .toArray(Record[]::new);

    postRecords(context, records);
    Record expectedRecord = records[0];

    List<Filter> filters = List.of(
      new Filter()
        .withValues(List.of("(OCoLC)63611770"))
        .withField("035")
        .withIndicator1("")
        .withIndicator2("")
        .withSubfield("a"),
      new Filter()
        .withValues(List.of(expectedRecord.getExternalIdsHolder().getInstanceId(), UUID.randomUUID().toString()))
        .withField("999")
        .withIndicator1("f")
        .withIndicator2("f")
        .withSubfield("i")
    );

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withLogicalOperator(AND)
        .withFilters(filters))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(expectedRecord.getId()))
      .body("identifiers[0].externalId", is(expectedRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchRecordByMultipleFiltersWithSpecifiedComparisonPartTypes() {
    List<Filter> filters = List.of(
      new Filter()
        .withValues(List.of("nin00009530412"))
        .withField("035")
        .withIndicator1("")
        .withIndicator2("")
        .withComparisonPartType(ALPHANUMERICS_ONLY)
        .withQualifier(ENDS_WITH)
        .withQualifierValue("00009530412")
        .withSubfield("a"),
      new Filter()
        .withValues(List.of("123"))
        .withField("024")
        .withIndicator1("8")
        .withIndicator2("0")
        .withSubfield("a")
        .withComparisonPartType(NUMERICS_ONLY)
        .withQualifier(BEGINS_WITH)
        .withQualifierValue("test")
    );

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withLogicalOperator(AND)
        .withFilters(filters))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

  @Test
  public void shouldMatchRecordIfMultipleFiltersSpecifiedAndLogicalOperatorIsOr() {
    List<Filter> filters = List.of(
      new Filter()
        .withValues(List.of("nin00009530412"))
        .withField("035")
        .withIndicator1("")
        .withIndicator2("")
        .withSubfield("a"),
      new Filter()
        .withValues(List.of("12345"))
        .withField("024")
        .withIndicator1("8")
        .withIndicator2("0")
        .withSubfield("a")
    );

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withLogicalOperator(OR)
        .withFilters(filters))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
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

  @Test
  public void shouldNotReturnTotalRecordsIfReturnTotalRecordsIsFalse(TestContext context) {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_WITH_035_FIELD_SAMPLE_PATH);
    int expectedRecordCount = 3;

    for (int i = 0; i < expectedRecordCount; i++) {
      String recordId = UUID.randomUUID().toString();
      Record record = new Record()
        .withId(recordId)
        .withMatchedId(recordId)
        .withSnapshotId(snapshot.getJobExecutionId())
        .withGeneration(0)
        .withRecordType(MARC_BIB)
        .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
        .withParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedRecordContent))
        .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()).withInstanceHrid(HR_ID));

      postRecords(context, record);
    }

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withReturnTotalRecordsCount(false)
        .withFilters(List.of(new Filter()
          .withValues(List.of("(OCoLC)63611770", "1234567"))
          .withField("035")
          .withIndicator1("")
          .withIndicator2("")
          .withSubfield("a"))))
      .post(RECORDS_MATCHING_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", nullValue())
      .body("identifiers.size()", is(expectedRecordCount));
  }

  @Test
  public void shouldNotReturnTotalRecordsIfReturnTotalRecordsIsFalseAndMatchingByMatchedIdField() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withReturnTotalRecordsCount(false)
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
      .body("totalRecords", nullValue())
      .body("identifiers.size()", is(1))
      .body("identifiers[0].recordId", is(existingRecord.getId()))
      .body("identifiers[0].externalId", is(existingRecord.getExternalIdsHolder().getInstanceId()));
  }

}
