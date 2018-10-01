package org.folio.rest.impl;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.RestVerticle;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(VertxUnitRunner.class)
public class RestVerticleTest {

  private static final String HOST = "http://localhost:";
  private static final String HTTP_PORT = "http.port";
  private static final String RECORD_STORAGE_PATH = "/record-storage/items/test";
  private static final String OKAPI_TENANT_HEADER = "x-okapi-tenant";
  private static final String TENANT = "diku";
  private static final String HEADER_ACCEPT = "Accept";
  private static final String ACCEPT_VALUES = "application/json, text/plain";
  private static final String APPLICATION_JSON = "application/json";
  private static final String HEADER_CONTENT_TYPE = "content-type";

  private Vertx vertx;
  private int port;

  @Before
  public void setUp(TestContext context) throws IOException {
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    DeploymentOptions options = new DeploymentOptions()
      .setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void testGetItemStub(TestContext context) {
    //TODO Replace testing stub
    final Async async = context.async();
    String url = HOST + port;
    String dataStorageTestUrl = url + RECORD_STORAGE_PATH;

    Handler<HttpClientResponse> handler = response -> {
      context.assertEquals(response.statusCode(), 200);
      context.assertEquals(response.headers().get(HEADER_CONTENT_TYPE), APPLICATION_JSON);
      response.handler(body -> {
        async.complete();
      });
    };
    sendRequest(dataStorageTestUrl, HttpMethod.GET, handler);
  }

  @Test
  public void testDeleteItemStub(TestContext context) {
    //TODO Replace testing stub
    final Async async = context.async();
    String url = HOST + port;
    String dataStorageTestUrl = url + RECORD_STORAGE_PATH;

    Handler<HttpClientResponse> handler = response -> {
      context.assertEquals(response.statusCode(), 204);
      async.complete();
    };
    sendRequest(dataStorageTestUrl, HttpMethod.DELETE, handler);
  }

  private void sendRequest(String url, HttpMethod method, Handler<HttpClientResponse> handler) {
    sendRequest(url, method, handler, "");
  }

  private void sendRequest(String url, HttpMethod method, Handler<HttpClientResponse> handler, String content) {
    Buffer buffer = Buffer.buffer(content);
    vertx.createHttpClient()
      .requestAbs(method, url, handler)
      .putHeader(OKAPI_TENANT_HEADER, TENANT)
      .putHeader(HEADER_ACCEPT, ACCEPT_VALUES)
      .end(buffer);
  }
}
