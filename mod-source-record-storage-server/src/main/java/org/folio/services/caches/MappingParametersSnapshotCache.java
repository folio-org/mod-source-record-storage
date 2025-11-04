package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
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

  @Autowired
  public MappingParametersSnapshotCache(Vertx vertx,
    @Value("${srs.mapping-params-cache.expiration.time.seconds:3600}") long cacheExpirationTime) {
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .recordStats()
      .buildAsync();
  }

  public Future<Optional<MappingParameters>> get(String jobExecutionId, OkapiConnectionParams params) {
    try {
      LOGGER.info("get:: Cache keys before retrieval: {}", cache.synchronous().asMap().keySet());
      CompletableFuture<Optional<MappingParameters>> cachedValue = cache.getIfPresent(jobExecutionId);
      if (cachedValue != null) {
        LOGGER.info("get:: Cache hit for jobExecutionId: '{}'", jobExecutionId);
      } else {
        LOGGER.info("get:: Cache miss for jobExecutionId: '{}', loading from source", jobExecutionId);
      }
      return Future.fromCompletionStage(cache.get(jobExecutionId, (key, executor) -> loadMappingParametersSnapshot(key, params)));
    } catch (Exception e) {
      LOGGER.warn("get:: Error loading MappingParametersSnapshot by jobExecutionId: '{}'", jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Optional<MappingParameters>> loadMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams params) {
    LOGGER.debug("loadMappingParametersSnapshot:: Trying to load MappingParametersSnapshot by jobExecutionId  '{}' for cache, okapi url: {}, tenantId: {}", jobExecutionId, params.getOkapiUrl(), params.getTenantId());
    return RestUtil.doRequestWithSystemUser(params, "/mapping-metadata/"+ jobExecutionId, HttpMethod.GET, null)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getResponse().statusCode() == HttpStatus.HTTP_OK.toInt()) {
          LOGGER.info("loadMappingParametersSnapshot:: MappingParametersSnapshot was loaded by jobExecutionId '{}'", jobExecutionId);
          try {
            MappingParameters mappingParameters = Json.decodeValue(httpResponse.getJson().getString("mappingParams"), MappingParameters.class);
            return CompletableFuture.completedFuture(Optional.of(mappingParameters));
          } catch (Exception e) {
            String errorMessage = String.format("loadMappingParametersSnapshot:: Failed to deserialize MappingParameters for jobExecutionId: '%s'. Response body: %s", jobExecutionId, httpResponse.getBody());
            LOGGER.error(errorMessage, e);
            return CompletableFuture.failedFuture(new CacheLoadingException(errorMessage, e));
          }
        } else if (httpResponse.getResponse().statusCode() == HttpStatus.HTTP_NOT_FOUND.toInt()) {
          LOGGER.warn("loadMappingParametersSnapshot:: MappingParametersSnapshot was not found by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("loadMappingParametersSnapshot:: Error loading MappingParametersSnapshot by jobExecutionId: '%s', status code: %s, response message: %s",
            jobExecutionId, httpResponse.getResponse().statusCode(), httpResponse.getBody());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
    });
  }
}
