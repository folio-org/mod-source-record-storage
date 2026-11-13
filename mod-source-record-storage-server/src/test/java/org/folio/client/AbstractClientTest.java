package org.folio.client;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Vertx;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractClientTest {
  protected static final String TENANT_ID = "diku";
  protected static final String TOKEN = "dummy";

  protected static Vertx vertx;
  public static WireMockServer wireMockServer;

  @BeforeAll
  public static void setUpClass() {
    vertx = Vertx.vertx();
    wireMockServer = new WireMockServer(new WireMockConfiguration().dynamicPort());
    wireMockServer.start();
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    CompletableFuture<Void> close = new CompletableFuture<>();
    vertx.close().onComplete(v -> {
      wireMockServer.stop();
      close.complete(null);
    });
    close.get(30, TimeUnit.SECONDS);
  }
}
