package org.folio.client;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.SubfieldModification;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.services.exceptions.InstanceLinksException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class LinksClientTest extends AbstractClientTest {

  private static final String LINKING_RULES_URL = "/linking-rules/instance-authority";
  private static final UrlPathPattern URL_PATH_PATTERN =
    new UrlPathPattern(new RegexPattern("/links/instances/.*"), true);

  private final ConnectionParams params = new ConnectionParams(Map.of(
    XOkapiHeaders.TENANT, TENANT_ID,
    XOkapiHeaders.TOKEN, "token",
    XOkapiHeaders.URL, wireMockServer.baseUrl()
  ));

  private final InstanceLinkClient client = new InstanceLinkClient();

  @Test
  void shouldReturnLinks(VertxTestContext testContext) {
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

    vertx.runOnContext(v ->
      client.getLinksByInstanceId(instanceId, params).whenComplete((result, thr) -> testContext.verify(() -> {
        assertNull(thr);
        assertTrue(result.isPresent());
        InstanceLinkDtoCollection actual = result.get();
        assertEquals(linkDtoCollection.getTotalRecords(), actual.getTotalRecords());
        List<Link> actualLinks = actual.getLinks();
        assertEquals(links.size(), actualLinks.size());
        assertEquals(links.getFirst().getId(), actualLinks.getFirst().getId());
        assertEquals(links.getFirst().getInstanceId(), actualLinks.getFirst().getInstanceId());
        assertEquals(links.getFirst().getAuthorityId(), actualLinks.getFirst().getAuthorityId());
        assertEquals(links.getFirst().getAuthorityNaturalId(), actualLinks.getFirst().getAuthorityNaturalId());
        assertEquals(links.getFirst().getLinkingRuleId(), actualLinks.getFirst().getLinkingRuleId());
        testContext.completeNow();
      }))
    );
  }

  @Test
  void shouldReturnEmptyLinksOnNotFound(VertxTestContext testContext) {
    String instanceId = UUID.randomUUID().toString();

    wireMockServer.stubFor(get(URL_PATH_PATTERN)
      .willReturn(WireMock.notFound()));

    vertx.runOnContext(v ->
      client.getLinksByInstanceId(instanceId, params).whenComplete((result, thr) -> testContext.verify(() -> {
        assertNull(thr);
        assertTrue(result.isEmpty());
        testContext.completeNow();
      }))
    );
  }

  @Test
  void shouldFailLinksOnUnknownCode(VertxTestContext testContext) {
    String instanceId = UUID.randomUUID().toString();

    wireMockServer.stubFor(get(URL_PATH_PATTERN)
      .willReturn(WireMock.badRequest()));

    vertx.runOnContext(v ->
      client.getLinksByInstanceId(instanceId, params).whenComplete((result, thr) -> testContext.verify(() -> {
        assertNull(result);
        assertTrue(thr.getCause() instanceof InstanceLinksException);
        assertTrue(thr.getMessage().contains(instanceId));
        testContext.completeNow();
      }))
    );
  }

  @Test
  void shouldReturnLinkingRules(VertxTestContext testContext) {
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

    vertx.runOnContext(v ->
      client.getLinkingRuleList(params).whenComplete((result, thr) -> testContext.verify(() -> {
        assertNull(thr);
        assertTrue(result.isPresent());
        List<LinkingRuleDto> actualLinkingRules = result.get();
        assertEquals(linkingRules.size(), actualLinkingRules.size());
        assertEquals(linkingRules.getFirst().getId(), actualLinkingRules.getFirst().getId());
        assertEquals(linkingRules.getFirst().getAuthorityField(), actualLinkingRules.getFirst().getAuthorityField());
        assertEquals(linkingRules.getFirst().getAuthoritySubfields(), actualLinkingRules.getFirst().getAuthoritySubfields());
        assertEquals(linkingRules.getFirst().getBibField(), actualLinkingRules.getFirst().getBibField());
        assertEquals(linkingRules.getFirst().getSubfieldModifications(), actualLinkingRules.getFirst().getSubfieldModifications());
        assertEquals(linkingRules.getFirst().getValidation(), actualLinkingRules.getFirst().getValidation());
        testContext.completeNow();
      }))
    );
  }

  @Test
  void shouldReturnEmptyLinkingRulesOnNotFound(VertxTestContext testContext) {
    wireMockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.notFound()));

    vertx.runOnContext(v ->
      client.getLinkingRuleList(params).whenComplete((result, thr) -> testContext.verify(() -> {
        assertNull(thr);
        assertTrue(result.isEmpty());
        testContext.completeNow();
      }))
    );
  }

  @Test
  void shouldFailLinkingRulesOnUnknownCode(VertxTestContext testContext) {
    wireMockServer.stubFor(get(urlPathEqualTo(LINKING_RULES_URL))
      .willReturn(WireMock.badRequest()));

    vertx.runOnContext(v ->
      client.getLinkingRuleList(params).whenComplete((result, thr) -> testContext.verify(() -> {
        assertNull(result);
        assertTrue(thr.getCause() instanceof InstanceLinksException);
        testContext.completeNow();
      }))
    );
  }
}
