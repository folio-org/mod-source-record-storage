package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.ws.rs.core.MediaType;
import java.io.IOException;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

@RunWith(VertxUnitRunner.class)
public class RestVerticleTest {

  private static final String HTTP_PORT = "http.port";

  private static final String TENANT_ID = "diku";
  private static final String ACCEPT_VALUES = "application/json, text/plain";
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final int CONTENT_LENGTH_DEFAULT = 1000;
  private static final String HOST = "http://localhost:";
  private static final String RECORD_STORAGE_PATH = "/record-storage";
  private Vertx vertx;
  private int port;

  private String baseServicePath;

  @Before
  public void setUp(TestContext context) throws IOException {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
    baseServicePath = HOST + port + RECORD_STORAGE_PATH;
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testGetRecordStorageItems(TestContext context) {
    //TODO Replace testing stub
    String serviceUrl = "/items";
    String dataStorageTestUrl = baseServicePath + serviceUrl;
    getDefaultGiven()
      .param("query", "query")
      .when().get(dataStorageTestUrl)
      .then().statusCode(200);
  }

  @Test
  public void testDeleteRecordStorageItemsByItemId(TestContext context) {
    //TODO Replace testing stub
    String serviceUrl = "/items/{itemId}";
    String dataStorageTestUrl = baseServicePath + serviceUrl;
    getDefaultGiven()
      .pathParam("itemId", "999")
      .when().delete(dataStorageTestUrl)
      .then().statusCode(204);
  }


  @Test
  public void testGetRecordStorageItemsByItemId(TestContext context) {
    //TODO Replace testing stub
    String serviceUrl = "/items/{itemId}";
    String dataStorageTestUrl = baseServicePath + serviceUrl;
    getDefaultGiven()
      .pathParam("itemId", "777")
      .when().get(dataStorageTestUrl)
      .then().statusCode(200);
  }

  @Test
  public void testGetRecordStorageLogs(TestContext context) {
    //TODO Replace testing stub
    String serviceUrl = "/logs";
    String dataStorageTestUrl = baseServicePath + serviceUrl;
    getDefaultGiven()
      .param("query", "query")
      .when().get(dataStorageTestUrl)
      .then().statusCode(200);
  }

  private RequestSpecification getDefaultGiven() {
    return RestAssured.given()
      .header(OKAPI_HEADER_TENANT, TENANT_ID)
      .header(HttpHeaders.ACCEPT.toString(), ACCEPT_VALUES)
      .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
      .header(CONTENT_LENGTH, CONTENT_LENGTH_DEFAULT);
  }
}
