package org.folio.rest.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.TestMarcRecordsCollection;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@RunWith(VertxUnitRunner.class)
public class TestMarcRecordsApiTest extends AbstractRestVerticleTest {

  private static final String POPULATE_TEST_MARK_RECORDS_PATH = "/source-storage/populate-test-marc-records";

  @Test
  @Ignore("Deprecated endpoint")
  public void shouldReturnNoContentOnPostRecordCollectionPassedInBody() throws IOException {
    RawRecord rawRecord = new RawRecord().withContent(
      new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));

    RestAssured.given()
      .spec(spec)
      .body(new TestMarcRecordsCollection()
        .withRawRecords(List.of(rawRecord)))
      .when()
      .post(POPULATE_TEST_MARK_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  @Ignore("Deprecated endpoint")
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
