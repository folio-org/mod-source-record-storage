package org.folio.client;


import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.util.Collections.singletonList;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.SubfieldModification;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.services.exceptions.InstanceLinksException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class LinksClientTest extends AbstractClientTest {

  private static final String LINKING_RULES_URL = "/linking-rules/instance-authority";
  private static final UrlPathPattern URL_PATH_PATTERN =
    new UrlPathPattern(new RegexPattern("/links/instances/.*"), true);

  private final OkapiConnectionParams params = new OkapiConnectionParams(Map.of(
    RestUtil.OKAPI_TENANT_HEADER, TENANT_ID,
    RestUtil.OKAPI_TOKEN_HEADER, "token",
    RestUtil.OKAPI_URL_HEADER, wireMockServer.baseUrl()
  ), vertx);

  private final InstanceLinkClient client = new InstanceLinkClient();

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();


  @Test
  public void shouldReturnLinks(TestContext context) {
    Async async = context.async();

    String instanceId = UUID.randomUUID().toString();
    List<Link> links = singletonList(new Link()
      .withId(1)
      .withInstanceId(UUID.randomUUID().toString())
      .withAuthorityId(UUID.randomUUID().toString())
      .withAuthorityNaturalId("test")
      .withLinkingRuleId(1));
    InstanceLinkDtoCollection linkDtoCollection = new InstanceLinkDtoCollection()
      .withLinks(links)
        .withTotalRecords(1);

    wireMockServer.stubFor(get(URL_PATH_PATTERN)
      .willReturn(WireMock.ok().withBody(Json.encode(linkDtoCollection))));

    CompletableFuture<Optional<InstanceLinkDtoCollection>> optionalFuture = client.getLinksByInstanceId(instanceId, params);

    optionalFuture.whenComplete((result, thr) -> {
      context.assertNull(thr);
      context.assertTrue(result.isPresent());
      InstanceLinkDtoCollection actualLinkDtoCollection = result.get();
      context.assertEquals(linkDtoCollection.getTotalRecords(), actualLinkDtoCollection.getTotalRecords());
      List<Link> actualLinks = actualLinkDtoCollection.getLinks();
      context.assertEquals(links.size(), actualLinks.size());
      context.assertEquals(links.get(0).getId(), actualLinks.get(0).getId());
      context.assertEquals(links.get(0).getInstanceId(), actualLinks.get(0).getInstanceId());
      context.assertEquals(links.get(0).getAuthorityId(), actualLinks.get(0).getAuthorityId());
      context.assertEquals(links.get(0).getAuthorityNaturalId(), actualLinks.get(0).getAuthorityNaturalId());
      context.assertEquals(links.get(0).getLinkingRuleId(), actualLinks.get(0).getLinkingRuleId());

      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyLinksOnNotFound(TestContext context) {
    Async async = context.async();

    String instanceId = UUID.randomUUID().toString();

    wireMockServer.stubFor(get(URL_PATH_PATTERN)
      .willReturn(WireMock.notFound()));

    CompletableFuture<Optional<InstanceLinkDtoCollection>> optionalFuture = client.getLinksByInstanceId(instanceId, params);

    optionalFuture.whenComplete((result, thr) -> {
      context.assertNull(thr);
      context.assertTrue(result.isEmpty());

      async.complete();
    });
  }

  @Test
  public void shouldFailLinksOnUnknownCode(TestContext context) {
    Async async = context.async();

    String instanceId = UUID.randomUUID().toString();

    wireMockServer.stubFor(get(URL_PATH_PATTERN)
      .willReturn(WireMock.badRequest()));

    CompletableFuture<Optional<InstanceLinkDtoCollection>> optionalFuture = client.getLinksByInstanceId(instanceId, params);

    optionalFuture.whenComplete((result, thr) -> {
      context.assertNull(result);
      context.assertTrue(thr.getCause() instanceof InstanceLinksException);
      context.assertTrue(thr.getMessage().contains(instanceId));

      async.complete();
    });
  }

  @Test
  public void shouldReturnLinkingRules(TestContext context) {
    Async async = context.async();

    List<LinkingRuleDto> linkingRules = singletonList(new LinkingRuleDto()
      .withId(1)
      .withBibField("100")
      .withAuthorityField("100")
      .withAuthoritySubfields(singletonList("a"))
      .withSubfieldModifications(singletonList(new SubfieldModification()
        .withSource("a")
        .withTarget("b"))));

    wireMockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.ok().withBody(Json.encode(linkingRules))));

    CompletableFuture<Optional<List<LinkingRuleDto>>> optionalFuture = client.getLinkingRuleList(params);

    optionalFuture.whenComplete((result, thr) -> {
      context.assertNull(thr);
      context.assertTrue(result.isPresent());
      List<LinkingRuleDto> actualLinkingRules = result.get();
      context.assertEquals(linkingRules.size(), actualLinkingRules.size());
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
  public void shouldReturnEmptyLinkingRulesOnNotFound(TestContext context) {
    Async async = context.async();

    wireMockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.notFound()));

    CompletableFuture<Optional<List<LinkingRuleDto>>> optionalFuture = client.getLinkingRuleList(params);

    optionalFuture.whenComplete((result, thr) -> {
      context.assertNull(thr);
      context.assertTrue(result.isEmpty());

      async.complete();
    });
  }

  @Test
  public void shouldFailLinkingRulesOnUnknownCode(TestContext context) {
    Async async = context.async();

    wireMockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.badRequest()));

    CompletableFuture<Optional<List<LinkingRuleDto>>> optionalFuture = client.getLinkingRuleList(params);

    optionalFuture.whenComplete((result, thr) -> {
      context.assertNull(result);
      context.assertTrue(thr.getCause() instanceof InstanceLinksException);

      async.complete();
    });
  }
}
