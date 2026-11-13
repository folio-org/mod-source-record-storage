package org.folio.services.caches;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.VerificationException;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LinkingRuleDto;
import org.folio.client.InstanceLinkClient;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class LinkingRulesCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String LINKING_RULES_URL = "/linking-rules/instance-authority";
  private static final int CACHE_EXPIRATION_TIME = 12;
  private static final List<LinkingRuleDto> linkingRules = singletonList(new LinkingRuleDto()
    .withId(1)
    .withBibField("100")
    .withAuthorityField("100")
    .withAuthoritySubfields(singletonList("a"))
    .withSubfieldModifications(emptyList()));

  public static WireMockServer mockServer;
  private static ConnectionParams params;

  private Vertx vertx;
  private final InstanceLinkClient instanceLinkClient = new InstanceLinkClient();
  private LinkingRulesCache linkingRulesCache;

  @BeforeAll
  static void setUp() {
    mockServer = new WireMockServer(new WireMockConfiguration().dynamicPort());
    mockServer.start();

    mockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.ok().withBody(Json.encode(linkingRules))));

    params = new ConnectionParams(Map.of(
      XOkapiHeaders.TENANT, TENANT_ID,
      XOkapiHeaders.TOKEN, "token",
      XOkapiHeaders.URL, mockServer.baseUrl()
    ));
  }

  @BeforeEach
  void initCache(Vertx vertx) {
    this.vertx = vertx;
    this.linkingRulesCache = new LinkingRulesCache(instanceLinkClient, vertx, CACHE_EXPIRATION_TIME);
  }

  @AfterAll
  static void tearDownClass() {
    mockServer.stop();
  }

  @AfterEach
  void tearDown() {
    mockServer.resetRequests();
  }

  @Test
  void shouldReturnLinkingRules(VertxTestContext testContext) {
    vertx.runOnContext(v -> {
      Future<Optional<List<LinkingRuleDto>>> optionalFuture = linkingRulesCache.get(params);

      optionalFuture.onComplete(testContext.succeeding(result -> {
        assertTrue(result.isPresent());
        List<LinkingRuleDto> actualLinkingRules = result.get();
        assertEquals(linkingRules.getFirst().getId(), actualLinkingRules.getFirst().getId());
        assertEquals(linkingRules.getFirst().getAuthorityField(), actualLinkingRules.getFirst().getAuthorityField());
        assertEquals(linkingRules.getFirst().getAuthoritySubfields(), actualLinkingRules.getFirst().getAuthoritySubfields());
        assertEquals(linkingRules.getFirst().getBibField(), actualLinkingRules.getFirst().getBibField());
        assertEquals(linkingRules.getFirst().getSubfieldModifications(), actualLinkingRules.getFirst().getSubfieldModifications());
        assertEquals(linkingRules.getFirst().getValidation(), actualLinkingRules.getFirst().getValidation());
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnLinkingRulesFromCache(VertxTestContext testContext) throws IllegalAccessException {
    FieldUtils.writeField(linkingRulesCache, "cache", Caffeine.newBuilder()
      .expireAfterWrite(2, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync(), true);

    vertx.runOnContext(v -> {
      Future<Optional<List<LinkingRuleDto>>> optionalFuture = linkingRulesCache.get(params);

      optionalFuture.onComplete(testContext.succeeding(result -> {
        assertTrue(result.isPresent());

        Future<Optional<List<LinkingRuleDto>>> optionalFuture1 = linkingRulesCache.get(params);

        optionalFuture1.onComplete(testContext.succeeding(result1 -> {
          assertTrue(result1.isPresent());

          List<LinkingRuleDto> actualLinkingRules = result1.get();

          assertEquals(linkingRules.getFirst().getId(), actualLinkingRules.getFirst().getId());
          assertEquals(linkingRules.getFirst().getAuthorityField(), actualLinkingRules.getFirst().getAuthorityField());
          assertEquals(linkingRules.getFirst().getAuthoritySubfields(), actualLinkingRules.getFirst().getAuthoritySubfields());
          assertEquals(linkingRules.getFirst().getBibField(), actualLinkingRules.getFirst().getBibField());
          assertEquals(linkingRules.getFirst().getSubfieldModifications(), actualLinkingRules.getFirst().getSubfieldModifications());
          assertEquals(linkingRules.getFirst().getValidation(), actualLinkingRules.getFirst().getValidation());

          try {
            mockServer.verify(1, getRequestedFor(urlPathEqualTo(LINKING_RULES_URL)));
          } catch (VerificationException e) {
            testContext.failNow(e);
            return;
          }

          testContext.completeNow();
        }));
      }));
    });
  }

  @Test
  void shouldFailOnException(VertxTestContext testContext) {
    ConnectionParams params = new ConnectionParams(emptyMap());

    vertx.runOnContext(v -> {
      Future<Optional<List<LinkingRuleDto>>> optionalFuture = linkingRulesCache.get(params);

      optionalFuture.onComplete(ar -> {
        assertTrue(ar.failed());
        assertInstanceOf(VertxException.class, ar.cause());
        testContext.completeNow();
      });
    });
  }

}
