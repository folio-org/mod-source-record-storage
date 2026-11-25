package org.folio.services.caches;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
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
import java.util.UUID;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.exceptions.CacheLoadingException;
import org.folio.services.exceptions.ProfileSnapshotNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;


@ExtendWith(VertxExtension.class)
public class JobProfileSnapshotCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final int CACHE_EXPIRATION_TIME = 3600;
  private static final int RETRY_ATTEMPTS = 3;
  private static final long RETRY_DELAY_MS = 10;

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
    jobProfileSnapshotCache = new JobProfileSnapshotCache(vertx,
      new JobProfileSnapshotCacheProperties(CACHE_EXPIRATION_TIME, RETRY_ATTEMPTS, RETRY_DELAY_MS));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(jobProfileSnapshot))));

    this.params = new ConnectionParams(Map.of(
      XOkapiHeaders.TENANT, TENANT_ID,
      XOkapiHeaders.TOKEN, "token",
      XOkapiHeaders.URL, mockServer.baseUrl()
    ));
  }

  private static final UrlPathPattern SNAPSHOT_URL_PATTERN =
    new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true);

  @DisplayName("should return profile snapshot when it is found")
  @Test
  void shouldReturnProfileSnapshot(VertxTestContext testContext) {
    vertx.runOnContext(v -> {
      // when
      Future<ProfileSnapshotWrapper> future = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      // then
      future.onComplete(testContext.succeeding(actual -> testContext.verify(() -> {
        assertThat(actual.getId()).isEqualTo(jobProfileSnapshot.getId());
        assertThat(actual.getChildSnapshotWrappers()).extracting(ProfileSnapshotWrapper::getId)
          .containsExactly(jobProfileSnapshot.getChildSnapshotWrappers().getFirst().getId());
        testContext.completeNow();
      })));
    });
  }

  @DisplayName("should fail with not found exception after all attempts when snapshot is always not found")
  @Test
  void shouldFailAfterAllAttempts_whenSnapshotIsAlwaysNotFound(VertxTestContext testContext) {
    // given
    WireMock.stubFor(get(SNAPSHOT_URL_PATTERN).willReturn(WireMock.notFound()));

    vertx.runOnContext(v -> {
      // when
      Future<ProfileSnapshotWrapper> future = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      // then
      future.onComplete(testContext.failing(error -> testContext.verify(() -> {
        assertThat(error).isInstanceOf(ProfileSnapshotNotFoundException.class);
        mockServer.verify(RETRY_ATTEMPTS, getRequestedFor(SNAPSHOT_URL_PATTERN));
        testContext.completeNow();
      })));
    });
  }

  @DisplayName("should return snapshot when first response is not found and the next one is ok")
  @Test
  void shouldReturnProfileSnapshot_whenNotFoundThenOk(VertxTestContext testContext) {
    // given
    WireMock.stubFor(get(SNAPSHOT_URL_PATTERN).inScenario("retry")
      .whenScenarioStateIs(Scenario.STARTED)
      .willReturn(WireMock.notFound())
      .willSetStateTo("created"));
    WireMock.stubFor(get(SNAPSHOT_URL_PATTERN).inScenario("retry")
      .whenScenarioStateIs("created")
      .willReturn(WireMock.ok().withBody(Json.encode(jobProfileSnapshot))));

    vertx.runOnContext(v -> {
      // when
      Future<ProfileSnapshotWrapper> future = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      // then
      future.onComplete(testContext.succeeding(actual -> testContext.verify(() -> {
        assertThat(actual.getId()).isEqualTo(jobProfileSnapshot.getId());
        mockServer.verify(2, getRequestedFor(SNAPSHOT_URL_PATTERN));
        testContext.completeNow();
      })));
    });
  }

  @DisplayName("should request snapshot again when previous get ended with not found")
  @Test
  void shouldNotCacheMiss_whenPreviousGetEndedWithNotFound(VertxTestContext testContext) {
    // given
    WireMock.stubFor(get(SNAPSHOT_URL_PATTERN).willReturn(WireMock.notFound()));

    vertx.runOnContext(v -> {
      // when
      jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params)
        .recover(error -> {
          WireMock.stubFor(get(SNAPSHOT_URL_PATTERN)
            .willReturn(WireMock.ok().withBody(Json.encode(jobProfileSnapshot))));
          return jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);
        })
        // then
        .onComplete(testContext.succeeding(actual -> testContext.verify(() -> {
          assertThat(actual.getId()).isEqualTo(jobProfileSnapshot.getId());
          mockServer.verify(RETRY_ATTEMPTS + 1, getRequestedFor(SNAPSHOT_URL_PATTERN));
          testContext.completeNow();
        })));
    });
  }

  @DisplayName("should fail immediately without retry when server error occurs")
  @Test
  void shouldFailWithoutRetry_whenServerErrorOccurs(VertxTestContext testContext) {
    // given
    WireMock.stubFor(get(SNAPSHOT_URL_PATTERN).willReturn(WireMock.serverError()));

    vertx.runOnContext(v -> {
      // when
      Future<ProfileSnapshotWrapper> future = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

      // then
      future.onComplete(testContext.failing(error -> testContext.verify(() -> {
        assertThat(error).isInstanceOf(CacheLoadingException.class);
        mockServer.verify(1, getRequestedFor(SNAPSHOT_URL_PATTERN));
        testContext.completeNow();
      })));
    });
  }

  @DisplayName("should fail when specified profile snapshot id is null")
  @Test
  void shouldReturnFailedFuture_whenSpecifiedProfileSnapshotIdIsNull(VertxTestContext testContext) {
    vertx.runOnContext(v -> {
      // when
      Future<ProfileSnapshotWrapper> future = jobProfileSnapshotCache.get(null, this.params);

      // then
      future.onComplete(testContext.failing(error -> testContext.completeNow()));
    });
  }

}
