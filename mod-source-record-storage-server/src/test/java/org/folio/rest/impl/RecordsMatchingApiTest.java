package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
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
  private static final String MARC_BIB_PARSED_CONTENT_WITH_ADDITIONAL_FIELDS = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{ \"001\": \"12345\" }, {\"007\": \"12569\"},{\"007\": \"1234567\"},{\"024\": {\"ind1\": \"8\", \"ind2\": \"0\", \"subfields\": [{\"a\": \"test123\"}]}}, {\"024\": {\"ind1\": \"1\", \"ind2\": \"1\", \"subfields\": [{\"a\": \"test45\"}]}},{\"035\": {\"ind1\": \" \", \"ind2\": \" \", \"subfields\": [{\"a\": \"nin00009530412\"}]}}, {\"035\": {\"ind1\": \" \", \"ind2\": \" \", \"subfields\": [{\"a\": \"12345\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"948\":{\"ind1\":\"\",\"ind2\":\"\",\"subfields\":[{\"a\":\"acf4f6e2-115c-4509-9d4c-536c758ef917\"},{\"b\":\"681394b4-10d8-4cb1-a618-0f9bd6152119\"},{\"d\":\"12345\"},{\"e\":\"lts\"},{\"x\":\"addfast\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"acf4f6e2-115c-4509-9d4c-536c758ef917\"}, {\"i\":\"681394b4-10d8-4cb1-a618-0f9bd6152119\"}]}}]}";
  private static final String PARSED_CONTENT_WITHOUT_999_FIELD = "{\"leader\": \"01589ccm a2200373   4500\", \"fields\": [{\"001\": \"12345\"}, {\"035\": {\"ind1\": \" \", \"ind2\": \" \", \"subfields\": [{\"a\": \"in00009530412\"}]}}, {\"245\": {\"ind1\": \"1\", \"ind2\": \"0\", \"subfields\": [{\"a\": \"Neue Ausgabe sämtlicher Werke,\"}]}}, {\"948\": {\"ind1\": \"\", \"ind2\": \"\", \"subfields\": [{\"a\": \"acf4f6e2-115c-4509-9d4c-536c758ef917\"}, {\"b\": \"681394b4-10d8-4cb1-a618-0f9bd6152119\"}, {\"d\": \"12345\"}, {\"e\": \"lts\"}, {\"x\": \"addfast\"}]}}]}";
  private static final String MARC_AUTHORITY_PARSED_CONTENT = "{\"leader\": \"01012cz  a2200241n  4500\", \"fields\": [{\"001\": \"sh 85001589\"}, {\"005\": \"20171119085041.0\"}, {\"008\": \"201001 n acanaaabn           n aaa     d\"}, {\"010\": {\"subfields\": [{\"a\": \"sh 85001589\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"024\": {\"subfields\": [{\"a\": \"0022-0469\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"035\": {\"subfields\": [{\"a\": \"90c37ff4-2f1e-451f-8822-87241b081617\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"100\": {\"subfields\": [{\"a\": \"Eimermacher, Karl\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"110\": {\"subfields\": [{\"a\": \"BR140\"}, {\"b\": \".J6\"}], \"ind1\": \"0\", \"ind2\": \" \"}}, {\"111\": {\"subfields\": [{\"a\": \"270.05\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"130\": {\"subfields\": [{\"a\": \"The Journal of ecclesiastical history\"}], \"ind1\": \"0\", \"ind2\": \"4\"}}, {\"150\": {\"subfields\": [{\"a\": \"The Journal of ecclesiastical history.\"}], \"ind1\": \"0\", \"ind2\": \"4\"}}, {\"999\": {\"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [{\"s\": \"b90cb1bc-601f-45d7-b99e-b11efd281dcd\"}, {\"i\": \"79653189-7d66-4203-9564-ba6677911e75\"}]}}]}";
  private static final String MARC_HOLDINGS_PARSED_CONTENT = "{\"leader\": \"01012cu  a2200241n  4500\", \"fields\": [{\"001\": \"1000649\"}, {\"004\": \"in00000000001\"}, {\"005\": \"20171119085041.0\"}, {\"008\": \"201001 n acanaaabn           n aaa     d\"}, {\"010\": {\"subfields\": [{\"a\": \"n   58020553 \"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"035\": {\"subfields\": [{\"a\": \"90c37ff4-2f1e-451f-8822-87241b081617\"}], \"ind1\": \" \", \"ind2\": \" \"}}, {\"999\": {\"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [{\"s\": \"b90cb1bc-601f-45d7-b99e-b11efd281dcd\"}, {\"i\": \"4795613b-c47c-4564-adaa-6379dd8c1d28\"}]}}]}";

  private static String rawRecordContent;

  private Snapshot snapshot;
  private Record existingRecord;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    rawRecordContent = new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class);
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
      .withParsedRecord(new ParsedRecord().withId(existingRecordId).withContent(MARC_BIB_PARSED_CONTENT_WITH_ADDITIONAL_FIELDS))
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
  public void shouldMatchMarcAuthorityRecordByAuthorityIdField(TestContext context) {
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(MARC_AUTHORITY_PARSED_CONTENT))
      .withExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(UUID.randomUUID().toString()));

    postRecords(context, record);
    shouldMatchRecordByExternalIdField(record);
  }

  @Test
  public void shouldMatchMarcHoldingsRecordByHoldingIdField(TestContext context) {
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_HOLDING)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(MARC_HOLDINGS_PARSED_CONTENT))
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
  public void shouldNotMatchRecordBy035FieldIfRecordExternalIdIsNull(TestContext context) {
    String recordId = UUID.randomUUID().toString();
    Record record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(new RawRecord().withId(recordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(recordId).withContent(PARSED_CONTENT_WITHOUT_999_FIELD));

    postRecords(context, record);

    RestAssured.given()
      .spec(spec)
      .when()
      .body(new RecordMatchingDto()
        .withRecordType(RecordMatchingDto.RecordType.MARC_BIB)
        .withFilters(List.of(new Filter()
          .withValues(List.of("in00009530412", "oclc1234567"))
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
        .withParsedRecord(new ParsedRecord().withId(recordId).withContent(PARSED_CONTENT_WITHOUT_999_FIELD))
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
          .withValues(List.of("in00009530412", "oclc1234567"))
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
