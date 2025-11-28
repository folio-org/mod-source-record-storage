package org.folio.services.caches;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.VerificationException;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LinkingRuleDto;
import org.folio.client.InstanceLinkClient;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
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
  private static Vertx vertx = Vertx.vertx();
  private static OkapiConnectionParams params;

  private final InstanceLinkClient instanceLinkClient = new InstanceLinkClient();
  private final LinkingRulesCache linkingRulesCache = new LinkingRulesCache(instanceLinkClient, vertx, CACHE_EXPIRATION_TIME);

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();


  @BeforeClass
  public static void setUp() {
    vertx = Vertx.vertx();

    mockServer = new WireMockServer(new WireMockConfiguration().dynamicPort());
    mockServer.start();

    mockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.ok().withBody(Json.encode(linkingRules))));

    params = new OkapiConnectionParams(Map.of(
      RestUtil.OKAPI_TENANT_HEADER, TENANT_ID,
      RestUtil.OKAPI_TOKEN_HEADER, "token",
      RestUtil.OKAPI_URL_HEADER, mockServer.baseUrl()
    ), vertx);
  }
  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      mockServer.stop();
      async.complete();
    }));
  }

  @After
  public void tearDown() {
    mockServer.resetRequests();
  }

  @Test
  public void shouldReturnLinkingRules(TestContext context) {
    Async async = context.async();

    Future<Optional<List<LinkingRuleDto>>> optionalFuture = linkingRulesCache.get(params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      List<LinkingRuleDto> actualLinkingRules = ar.result().get();
      context.assertEquals(linkingRules.get(0).getId(), actualLinkingRules.get(0).getId());
      context.assertEquals(linkingRules.get(0).getAuthorityField(), actualLinkingRules.get(0).getAuthorityField());
      context.assertEquals(linkingRules.get(0).getAuthoritySubfields(), actualLinkingRules.get(0).getAuthoritySubfields());
      context.assertEquals(linkingRules.get(0).getBibField(), actualLinkingRules.get(0).getBibField());
      context.assertEquals(linkingRules.get(0).getSubfieldModifications(), actualLinkingRules.get(0).getSubfieldModifications());
      context.assertEquals(linkingRules.get(0).getValidation(), actualLinkingRules.get(0).getValidation());
      async.complete();
    });
  }

  @Test
  public void shouldReturnLinkingRulesFromCache(TestContext context) throws IllegalAccessException {
    Async async = context.async();
    FieldUtils.writeField(linkingRulesCache, "cache", Caffeine.newBuilder()
      .expireAfterWrite(2, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync(), true);

    Future<Optional<List<LinkingRuleDto>>> optionalFuture = linkingRulesCache.get(params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());

      Future<Optional<List<LinkingRuleDto>>> optionalFuture1 = linkingRulesCache.get(params);

      optionalFuture1.onComplete(ar1 -> {
        context.assertTrue(ar1.succeeded());
        context.assertTrue(ar1.result().isPresent());

        List<LinkingRuleDto> actualLinkingRules = ar1.result().get();

        context.assertEquals(linkingRules.get(0).getId(), actualLinkingRules.get(0).getId());
        context.assertEquals(linkingRules.get(0).getAuthorityField(), actualLinkingRules.get(0).getAuthorityField());
        context.assertEquals(linkingRules.get(0).getAuthoritySubfields(), actualLinkingRules.get(0).getAuthoritySubfields());
        context.assertEquals(linkingRules.get(0).getBibField(), actualLinkingRules.get(0).getBibField());
        context.assertEquals(linkingRules.get(0).getSubfieldModifications(), actualLinkingRules.get(0).getSubfieldModifications());
        context.assertEquals(linkingRules.get(0).getValidation(), actualLinkingRules.get(0).getValidation());

        try {
          mockServer.verify(1, getRequestedFor(urlPathEqualTo(LINKING_RULES_URL)));
        } catch (VerificationException e) {
          context.fail(e);
        }

        async.complete();
      });
    });
  }

  @Test
  public void shouldFailOnException(TestContext context) {
    Async async = context.async();

    OkapiConnectionParams params = new OkapiConnectionParams(emptyMap(), vertx);

    Future<Optional<List<LinkingRuleDto>>> optionalFuture = linkingRulesCache.get(params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getCause() instanceof VertxException);

      async.complete();
    });
  }

}
