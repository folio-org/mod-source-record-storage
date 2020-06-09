package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@RunWith(VertxUnitRunner.class)
public class SampleRecordsTest extends AbstractRestVerticleTest {

  private static final String SOURCE_STORAGE_SOURCE_RECORDS_PATH = "/source-storage/sourceRecords";

  @Test
  public void shouldReturnListWithSampleData() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(11))
      .body("sourceRecords", notNullValue());
  }

}
