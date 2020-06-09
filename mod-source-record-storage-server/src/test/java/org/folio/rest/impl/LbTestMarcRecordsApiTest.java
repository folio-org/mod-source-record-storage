package org.folio.rest.impl;

import java.util.Collections;

import org.apache.http.HttpStatus;
import org.folio.TestMocks;
import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.LbSnapshotDaoUtil;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.TestMarcRecordsCollection;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.restassured.RestAssured;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LbTestMarcRecordsApiTest extends AbstractRestVerticleTest {

  private static final String POPULATE_TEST_MARK_RECORDS_PATH = "/lb-source-storage/populate-test-marc-records";

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    ReactiveClassicGenericQueryExecutor qe = PostgresClientFactory.getQueryExecutor(vertx, TENANT_ID);
    LbSnapshotDaoUtil.deleteAll(qe).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      LbSnapshotDaoUtil.save(qe, ModTenantAPI.STUB_SNAPSHOT).onComplete(save -> {
        if (delete.failed()) {
          context.fail(delete.cause());
        }
        async.complete();
      });
    });
  }

  @Test
  public void shouldReturnNoContentOnPostRecordCollectionPassedInBody() {
    RestAssured.given()
      .spec(spec)
      .body(new TestMarcRecordsCollection()
        .withRawRecords(TestMocks.getRawRecords()))
      .when()
      .post(POPULATE_TEST_MARK_RECORDS_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
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
