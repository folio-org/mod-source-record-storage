package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.TestMarcRecordsCollection;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

@RunWith(VertxUnitRunner.class)
public class TestMarcRecordsApiTest extends AbstractRestVerticleTest {

  private static final String POPULATE_TEST_MARK_RECORDS_PATH = "/source-storage/populate-test-marc-records";
  private static final String RECORDS_TABLE_NAME = "records";
  private static final String RAW_RECORDS_TABLE_NAME = "raw_records";
  private static final String ERROR_RECORDS_TABLE_NAME = "error_records";
  private static final String MARC_RECORDS_TABLE_NAME = "marc_records";
  private static RawRecord rawRecord_1;
  private static RawRecord rawRecord_2;

  static {
    try {
      rawRecord_1 = new RawRecord()
        .withId("9b0ae8c4-2e0c-11e9-b210-d663bd873d93")
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_RECORD_CONTENT_SAMPLE_PATH), String.class));
      rawRecord_2 = new RawRecord()
        .withId("9b0aec8e-2e0c-11e9-b210-d663bd873d93")
        .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class).encode());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  private static TestMarcRecordsCollection recordCollection = new TestMarcRecordsCollection()
    .withRawRecords(Arrays.asList(rawRecord_1, rawRecord_2));

  @Before
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    pgClient.delete(RECORDS_TABLE_NAME, new Criterion(), event -> {
      pgClient.delete(RAW_RECORDS_TABLE_NAME, new Criterion(), event1 -> {
        pgClient.delete(ERROR_RECORDS_TABLE_NAME, new Criterion(), event2 -> {
          pgClient.delete(MARC_RECORDS_TABLE_NAME, new Criterion(), event3 -> {
            if (event3.failed()) {
              context.fail(event3.cause());
            }
            async.complete();
          });
        });
      });
    });
  }

  @Test
  public void shouldReturnNoContentOnPostRecordCollectionPassedInBody(TestContext testContext) {
    Async async = testContext.async();
    RestAssured.given()
      .spec(spec)
      .body(recordCollection)
      .when()
      .post(POPULATE_TEST_MARK_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
    async.complete();
  }

  @Test
  public void shouldReturnUnprocessableEntityOnPostWhenNoRecordCollectionPassedInBody() {
    TestMarcRecordsCollection testMarcRecordsCollection = new TestMarcRecordsCollection()
      .withRawRecords(Collections.singletonList(new RawRecord()));

    RestAssured.given()
      .spec(spec)
      .body(testMarcRecordsCollection)
      .when()
      .post(POPULATE_TEST_MARK_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

    RestAssured.given()
      .spec(spec)
      .when()
      .post(POPULATE_TEST_MARK_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }
}
