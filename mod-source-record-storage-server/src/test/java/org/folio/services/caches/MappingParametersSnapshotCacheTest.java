package org.folio.services.caches;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.RunTestOnContext;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.services.exceptions.CacheLoadingException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(VertxExtension.class)
class MappingParametersSnapshotCacheTest {

  private static final int CACHE_EXPIRATION_TIME = 3600;
  private static final int MAX_RETRIES = 3;

  @RegisterExtension
  static RunTestOnContext runTestOnContext = new RunTestOnContext();

  private MappingParametersSnapshotCache cache;
  private static Vertx vertx;
  static OkapiConnectionParams params;
  private static Map<String, Integer> requestCounts;

  @BeforeAll
  static void setUp(VertxTestContext vtc) {
    vertx = runTestOnContext.vertx();
    requestCounts = new ConcurrentHashMap<>();

    vertx.createHttpServer()
      .requestHandler(req -> {
        String path = req.path();
        requestCounts.merge(path, 1, Integer::sum);

        switch (path) {
          case "/mapping-metadata/200":
            var mappingParameters200 = new MappingParameters().withTenantConfigurationZone("ozone");
            var mappingMetadata200 = new MappingMetadataDto().withMappingParams(Json.encode(mappingParameters200));
            req.response().setStatusCode(200).end(Json.encode(mappingMetadata200));
            return;
          case "/mapping-metadata/404-always":
            req.response().setStatusCode(404).end("not found");
            return;
          case "/mapping-metadata/404-then-200":
            if (requestCounts.get(path) < 3) {
              req.response().setStatusCode(404).end("not found yet");
            } else {
              var mappingParametersRetry = new MappingParameters().withTenantConfigurationZone("retry-success");
              var mappingMetadataRetry = new MappingMetadataDto().withMappingParams(Json.encode(mappingParametersRetry));
              req.response().setStatusCode(200).end(Json.encode(mappingMetadataRetry));
            }
            return;
          case "/mapping-metadata/cached-id":
            var mappingParametersCache = new MappingParameters().withTenantConfigurationZone("cached-zone");
            var mappingMetadataCache = new MappingMetadataDto().withMappingParams(Json.encode(mappingParametersCache));
            req.response().setStatusCode(200).end(Json.encode(mappingMetadataCache));
            return;
          default:
            req.response().setStatusCode(500).end("internal server error for path " + path);
        }
      })
      .listen(0)
      .onComplete(vtc.succeeding(server -> {
        Map<String, String> okapiHeaders = Map.of("x-okapi-url", "http://localhost:" + server.actualPort());
        params = new OkapiConnectionParams(okapiHeaders, vertx);
        vtc.completeNow();
      }));
  }

  @BeforeEach
  void resetCacheAndCounts() {
    cache = new MappingParametersSnapshotCache(vertx, CACHE_EXPIRATION_TIME);
    requestCounts.clear();
  }

  @Test
  void shouldSucceedOnFirstAttempt(VertxTestContext vtc) {
    cache.get("200", params)
    .onComplete(vtc.succeeding(result -> {
      var mappingParameters = result.get();
      assertThat(mappingParameters.getTenantConfigurationZone(), is("ozone"));
      assertEquals(1, requestCounts.get("/mapping-metadata/200"));
      vtc.completeNow();
    }));
  }

  @Test
  void shouldReturnEmptyAfterAllRetries(VertxTestContext vtc) {
    cache.get("404-always", params)
    .onComplete(vtc.succeeding(result -> {
      assertThat(result.isEmpty(), is(true));
      assertEquals(MAX_RETRIES + 1, requestCounts.get("/mapping-metadata/404-always"));
      vtc.completeNow();
    }));
  }

  @Test
  void shouldFailImmediatelyOnServerError(VertxTestContext vtc) {
    cache.get("999", params)
    .onComplete(vtc.failing(e -> {
      assertThat(e, is(instanceOf(CompletionException.class)));
      var cause = e.getCause();
      assertThat(cause, is(instanceOf(CacheLoadingException.class)));
      assertThat(cause.getMessage(), containsString("jobExecutionId: '999', status code: 500"));
      assertEquals(1, requestCounts.get("/mapping-metadata/999"));
      vtc.completeNow();
    }));
  }

  @Test
  void shouldFailOnNullId(VertxTestContext vtc) {
    cache.get(null, params)
    .onComplete(vtc.failing(e -> {
      assertThat(e, is(instanceOf(NullPointerException.class)));
      vtc.completeNow();
    }));
  }

  @Test
  void shouldSucceedAfterRetryingOn404(VertxTestContext vtc) {
    cache.get("404-then-200", params)
      .onComplete(vtc.succeeding(result -> {
        assertTrue(result.isPresent());
        assertEquals("retry-success", result.get().getTenantConfigurationZone());
        assertEquals(3, requestCounts.get("/mapping-metadata/404-then-200"));
        vtc.completeNow();
      }));
  }

  @Test
  void shouldReturnFromCacheOnSecondCall(VertxTestContext vtc) {
    String jobExecutionId = "cached-id";
    String path = "/mapping-metadata/" + jobExecutionId;

    cache.get(jobExecutionId, params)
      .compose(firstResult -> {
        assertTrue(firstResult.isPresent());
        assertEquals("cached-zone", firstResult.get().getTenantConfigurationZone());
        assertEquals(1, requestCounts.get(path));

        return cache.get(jobExecutionId, params);
      })
      .onComplete(vtc.succeeding(secondResult -> {
        assertTrue(secondResult.isPresent());
        assertEquals("cached-zone", secondResult.get().getTenantConfigurationZone());
        assertEquals(1, requestCounts.get(path));
        vtc.completeNow();
      }));
  }

}
