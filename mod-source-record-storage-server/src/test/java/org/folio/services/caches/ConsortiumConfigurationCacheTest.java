package org.folio.services.caches;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.Optional;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.services.entities.ConsortiumConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(VertxExtension.class)
public class ConsortiumConfigurationCacheTest {
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID = "centralTenantId";
  private static final String CONSORTIUM_ID = "consortiumId";
  private static final String USER_TENANTS_ENDPOINT = "/user-tenants";
  private Vertx vertx;
  private ConsortiumConfigurationCache consortiumConfigurationCache;
  private ConnectionParams params;
  private final JsonObject consortiumConfiguration = new JsonObject()
    .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", CENTRAL_TENANT_ID).put("consortiumId", CONSORTIUM_ID)));

  @RegisterExtension
  WireMockExtension mockServer = WireMockExtension.newInstance()
    .configureStaticDsl(true)
    .options(WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)))
    .build();

  @BeforeEach
  void setUp(Vertx vertx) {
    this.vertx = vertx;
    this.consortiumConfigurationCache = new ConsortiumConfigurationCache(vertx, 3600);
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_ENDPOINT), true))
      .willReturn(WireMock.ok().withBody(consortiumConfiguration.encode())));

    this.params = new ConnectionParams(Map.of(
      XOkapiHeaders.TENANT, TENANT_ID,
      XOkapiHeaders.TOKEN, "token",
      XOkapiHeaders.URL, mockServer.baseUrl()
    ));
  }

  @Test
  void shouldReturnConsortiumConfiguration(VertxTestContext testContext) {
    vertx.runOnContext(v -> {
      Future<Optional<ConsortiumConfiguration>> optionalFuture = consortiumConfigurationCache.get(this.params);

      optionalFuture.onComplete(testContext.succeeding(result -> {
        assertTrue(result.isPresent());
        ConsortiumConfiguration actualConsortiumConfiguration = result.get();
        assertEquals(actualConsortiumConfiguration.getCentralTenantId(), CENTRAL_TENANT_ID);
        assertEquals(actualConsortiumConfiguration.getConsortiumId(), CONSORTIUM_ID);
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnEmptyOptionalWhenGetNotFoundOnConfigurationLoading(VertxTestContext testContext) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_ENDPOINT), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject().put("userTenants", new JsonArray())))));

    vertx.runOnContext(v -> {
      Future<Optional<ConsortiumConfiguration>> optionalFuture = consortiumConfigurationCache.get(this.params);

      optionalFuture.onComplete(testContext.succeeding(result -> {
        assertTrue(result.isEmpty());
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorOnConfigurationLoading(VertxTestContext testContext) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(USER_TENANTS_ENDPOINT), true))
      .willReturn(WireMock.serverError()));

    vertx.runOnContext(v -> {
      Future<Optional<ConsortiumConfiguration>> optionalFuture = consortiumConfigurationCache.get(this.params);

      optionalFuture.onComplete(ar -> {
        assertTrue(ar.failed());
        testContext.completeNow();
      });
    });
  }
}
