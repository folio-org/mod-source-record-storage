package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.folio.rest.impl.RecordApiTest.SOURCE_STORAGE_SOURCE_RECORDS_PATH;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@RunWith(VertxUnitRunner.class)
public class SampleRecordsTest extends AbstractRestVerticleTest {

  @Override
  public void clearTables(TestContext context) {
  }

  @Test
  public void shouldReturnListWithSampleData() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SOURCE_STORAGE_SOURCE_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(10))
      .body("sourceRecords", notNullValue());
  }
}
