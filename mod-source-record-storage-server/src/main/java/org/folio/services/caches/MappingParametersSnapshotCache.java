package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.services.exceptions.CacheLoadingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class MappingParametersSnapshotCache {

  private static final Logger LOGGER = LogManager.getLogger();

  private final AsyncCache<String, Optional<MappingParameters>> cache;
  private static final int MAX_RETRIES = 3;
  private static final long INITIAL_DELAY_MS = 200;
  private final Vertx vertx;

  @Autowired
  public MappingParametersSnapshotCache(Vertx vertx,
                                        @Value("${srs.mapping-params-cache.expiration.time.seconds:3600}") long cacheExpirationTime) {
    this.vertx = vertx;
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .recordStats()
      .buildAsync();
  }

  public Future<Optional<MappingParameters>> get(String jobExecutionId, OkapiConnectionParams params) {

    if (jobExecutionId == null) {
      LOGGER.warn("get:: Attempted to retrieve from cache with a null jobExecutionId.");
      return Future.failedFuture(new NullPointerException("jobExecutionId cannot be null"));
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("get:: Retrieving mapping parameters for jobExecutionId: '{}'", jobExecutionId);
      LOGGER.debug("get:: Current cache keys: {}", cache.synchronous().asMap().keySet());
      logCacheStats();
    }

    return Future.fromCompletionStage(
      cache.get(jobExecutionId, (key, executor) -> loadMappingParametersSnapshot(key, params))
    );
  }

  private void logCacheStats() {
    if (LOGGER.isDebugEnabled()) {
      CacheStats stats = cache.synchronous().stats();
      LOGGER.debug("Cache Statistics:");
      LOGGER.debug("  Request Count: {}", stats.requestCount());
      LOGGER.debug("  Hit Count: {}", stats.hitCount());
      LOGGER.debug("  Hit Rate: {}%", String.format("%.2f", stats.hitRate() * 100));
      LOGGER.debug("  Miss Count: {}", stats.missCount());
      LOGGER.debug("  Miss Rate: {}%", String.format("%.2f", stats.missRate() * 100));
      LOGGER.debug("  Load Count: {}", stats.loadCount());
      LOGGER.debug("  Average Load Time: {}%", String.format("%.2f", stats.averageLoadPenalty() / 1_000_000.0));
      LOGGER.debug("  Eviction Count: {}", stats.evictionCount());
    }
  }

  private CompletableFuture<Optional<MappingParameters>> loadMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams params) {
    return loadWithRetry(jobExecutionId, params, 0, INITIAL_DELAY_MS);
  }

  private CompletableFuture<Optional<MappingParameters>> loadWithRetry(String jobExecutionId, OkapiConnectionParams params, int attempt, long delay) {
    LOGGER.debug("loadWithRetry:: Attempt {} to load MappingParametersSnapshot for jobExecutionId '{}'", attempt + 1, jobExecutionId);

    return RestUtil.doRequestWithSystemUser(params, "/mapping-metadata/" + jobExecutionId, HttpMethod.GET, null)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        int statusCode = httpResponse.getResponse().statusCode();

        if (statusCode == HttpStatus.HTTP_OK.toInt()) {
          LOGGER.info("loadWithRetry:: Successfully loaded MappingParametersSnapshot on attempt {} for jobExecutionId '{}'", attempt + 1, jobExecutionId);
          try {
            MappingParameters mappingParameters = Json.decodeValue(httpResponse.getJson().getString("mappingParams"), MappingParameters.class);
            return CompletableFuture.completedFuture(Optional.of(mappingParameters));
          } catch (Exception e) {
            String errorMessage = String.format("loadWithRetry:: Failed to deserialize on attempt %d for jobExecutionId: '%s'. Response body: %s", attempt + 1, jobExecutionId, httpResponse.getBody());
            LOGGER.error(errorMessage, e);
            return CompletableFuture.failedFuture(new CacheLoadingException(errorMessage, e));
          }
        } else if (statusCode == HttpStatus.HTTP_NOT_FOUND.toInt()) {
          if (attempt < MAX_RETRIES) {
            LOGGER.warn("loadWithRetry:: Got 404 on attempt {}. Retrying in {} ms for jobExecutionId '{}'...", attempt + 1, delay, jobExecutionId);
            CompletableFuture<Optional<MappingParameters>> nextAttempt = new CompletableFuture<>();
            vertx.setTimer(delay, id -> {
              loadWithRetry(jobExecutionId, params, attempt + 1, delay * 2)
                .whenComplete((result, error) -> {
                  if (error != null) {
                    nextAttempt.completeExceptionally(error);
                  } else {
                    nextAttempt.complete(result);
                  }
                });
            });
            return nextAttempt;
          } else {
            LOGGER.error("loadWithRetry:: Got 404 on final attempt {}. Giving up for jobExecutionId '{}'.", attempt + 1, jobExecutionId);
            return CompletableFuture.completedFuture(Optional.empty());
          }
        } else {
          String message = String.format("loadWithRetry:: Error on attempt %d for jobExecutionId: '%s', status code: %s, response message: %s",
            attempt + 1, jobExecutionId, statusCode, httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }
}
