package org.folio.rest.impl;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LbSampleRecordsTest extends AbstractRestVerticleTest {

  @Override
  public void clearTables(TestContext context) {
    // do nothing
  }

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
