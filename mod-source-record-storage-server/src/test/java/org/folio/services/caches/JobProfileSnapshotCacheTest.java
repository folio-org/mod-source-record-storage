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
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;


@RunWith(VertxUnitRunner.class)
public class JobProfileSnapshotCacheTest {

  private static final String TENANT_ID = "diku";
  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final int CACHE_EXPIRATION_TIME = 3600;

  private final Vertx vertx = Vertx.vertx();
  private JobProfileSnapshotCache jobProfileSnapshotCache;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  ProfileSnapshotWrapper jobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)));

  private OkapiConnectionParams params;

  @Before
  public void setUp() {
    jobProfileSnapshotCache = new JobProfileSnapshotCache(vertx, CACHE_EXPIRATION_TIME);
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(jobProfileSnapshot))));

    this.params = new OkapiConnectionParams(Map.of(
      RestUtil.OKAPI_TENANT_HEADER, TENANT_ID,
      RestUtil.OKAPI_TOKEN_HEADER, "token",
      RestUtil.OKAPI_URL_HEADER, mockServer.baseUrl()
    ), vertx);
  }

  @Test
  public void shouldReturnProfileSnapshot(TestContext context) {
    Async async = context.async();

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      ProfileSnapshotWrapper actualProfileSnapshot = ar.result().get();
      context.assertEquals(jobProfileSnapshot.getId(), actualProfileSnapshot.getId());
      context.assertFalse(actualProfileSnapshot.getChildSnapshotWrappers().isEmpty());
      context.assertEquals(jobProfileSnapshot.getChildSnapshotWrappers().get(0).getId(),
        actualProfileSnapshot.getChildSnapshotWrappers().get(0).getId());
      async.complete();
    });
  }

  @Test
  public void shouldReturnEmptyOptionalWhenGetNotFoundOnSnapshotLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.notFound()));

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenGetServerErrorOnSnapshotLoading(TestContext context) {
    Async async = context.async();
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.serverError()));

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(jobProfileSnapshot.getId(), this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenSpecifiedProfileSnapshotIdIsNull(TestContext context) {
    Async async = context.async();

    Future<Optional<ProfileSnapshotWrapper>> optionalFuture = jobProfileSnapshotCache.get(null, this.params);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

}
