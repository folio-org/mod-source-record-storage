package org.folio.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractClientTest {
  protected static final String TENANT_ID = "diku";
  protected static final String TOKEN = "dummy";

  protected static Vertx vertx;
  public static WireMockServer wireMockServer;

  @BeforeClass
  public static void setUpClass() throws Exception {
    vertx = Vertx.vertx();

    wireMockServer = new WireMockServer(new WireMockConfiguration().dynamicPort());
    wireMockServer.start();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close().onComplete(context.asyncAssertSuccess(res -> {
      wireMockServer.stop();
      async.complete();
    }));
  }
}
