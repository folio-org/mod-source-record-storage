package org.folio.services.caches;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;


@ExtendWith(VertxExtension.class)
public class JobProfileSnapshotCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final int CACHE_EXPIRATION_TIME = 3600;

  private Vertx vertx;
  private JobProfileSnapshotCache jobProfileSnapshotCache;

  @RegisterExtension
  WireMockExtension mockServer = WireMockExtension.newInstance()
    .configureStaticDsl(true)
    .options(WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)))
    .build();

  ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)));

  private ConnectionParams params;

  @BeforeEach
  void setUp(Vertx vertx) {
    this.vertx = vertx;
    jobProfileSnapshotCache = new JobProfileSnapshotCache(vertx, CACHE_EXPIRATION_TIME);
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(jobProfileSnapshot))));

    this.params = new ConnectionParams(Map.of(
      XOkapiHeaders.TENANT, TENANT_ID,
      XOkapiHeaders.TOKEN, "token",
      XOkapiHeaders.URL, mockServer.baseUrl()
    ));
  }

  @Test
  void shouldReturnProfileSnapshot(VertxTestContext testContext) {
    vertx.runOnContext(v -> {
      Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      optionalFuture.onComplete(testContext.succeeding(result -> {
        assertTrue(result.isPresent());
        ProfileSnapshotWrapper actualProfileSnapshot = result.get();
        assertEquals(jobProfileSnapshot.getId(), actualProfileSnapshot.getId());
        assertFalse(actualProfileSnapshot.getChildSnapshotWrappers().isEmpty());
        assertEquals(jobProfileSnapshot.getChildSnapshotWrappers().getFirst().getId(),
          actualProfileSnapshot.getChildSnapshotWrappers().getFirst().getId());
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnEmptyOptionalWhenGetNotFoundOnSnapshotLoading(VertxTestContext testContext) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.notFound()));

    vertx.runOnContext(v -> {
      Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      optionalFuture.onComplete(testContext.succeeding(result -> {
        assertTrue(result.isEmpty());
        testContext.completeNow();
      }));
    });
  }

  @Test
  void shouldReturnFailedFutureWhenGetServerErrorOnSnapshotLoading(VertxTestContext testContext) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    vertx.runOnContext(v -> {
      Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      optionalFuture.onComplete(ar -> {
        assertTrue(ar.failed());
        testContext.completeNow();
      });
    });
  }

  @Test
  void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNull(VertxTestContext testContext) {
    vertx.runOnContext(v -> {
      Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(null, this.params);

      optionalFuture.onComplete(ar -> {
        assertTrue(ar.failed());
        testContext.completeNow();
      });
    });
  }

}
