package org.folio.services.caches;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.services.entities.ConsortiumConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.get;

@RunWith(VertxUnitRunner.class)
public class ConsortiumConfigurationCacheTest {
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "centralTenantId";
  private static final String CONSORTIUM_ID = "consortiumId";
  private static final String USER_TENANTS_ENDPOINT = "/user-tenants";
  private final Vertx vertx = Vertx.vertx();
  private final ConsortiumConfigurationCache consortiumConfigurationCache = new ConsortiumConfigurationCache(vertx);
  private OkapiConnectionParams params;
  private final JsonObject consortiumConfiguration = new JsonObject()
    .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", CENTRAL_TENANT_ID).put("consortiumId", CONSORTIUM_ID)));

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_ENDPOINT), true))
      .willReturn(WireMock.ok().withBody(consortiumConfiguration.encode())));

    this.params = new OkapiConnectionParams(Map.of(
      RestUtil.OKAPI_TENANT_HEADER, TENANT_ID,
      RestUtil.OKAPI_TOKEN_HEADER, "token",
      RestUtil.OKAPI_URL_HEADER, mockServer.baseUrl()
    ), vertx);
  }

  @Test
  public void shouldReturnConsortiumConfiguration(TestContext context) {
    Async async = context.async();

    Future<Optional<ConsortiumConfiguration>> optionalFuture = consortiumConfigurationCache.get(this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      ConsortiumConfiguration actualConsortiumConfiguration = ar.result().get();
      context.assertEquals(actualConsortiumConfiguration.getCentralTenantId(), CENTRAL_TENANT_ID);
      context.assertEquals(actualConsortiumConfiguration.getConsortiumId(), CONSORTIUM_ID);
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalWhenGetNotFoundOnConfigurationLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_ENDPOINT), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject().put("userTenants", new JsonArray())))));

    Future<Optional<ConsortiumConfiguration>> optionalFuture = consortiumConfigurationCache.get(this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenGetServerErrorOnConfigurationLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_ENDPOINT), true))
      .willReturn(WireMock.serverError()));

    Future<Optional<ConsortiumConfiguration>> optionalFuture = consortiumConfigurationCache.get(this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
