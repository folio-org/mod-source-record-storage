package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.MappingMetadataClient;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
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

  @Value("${srs.mapping-params-cache.expiration.time.seconds:3600}")
  private long cacheExpirationTime;
  private AsyncCache<String, Optional<MappingParameters>> cache;

  @Autowired
  public MappingParametersSnapshotCache(Vertx vertx) {
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  public Future<Optional<MappingParameters>> get(String jobExecutionId, OkapiConnectionParams params) {
    try {
      return Future.fromCompletionStage(cache.get(jobExecutionId, (key, executor) -> loadMappingParametersSnapshot(key, params)));
    } catch (Exception e) {
      LOGGER.warn("Error loading MappingParametersSnapshot by jobExecutionId: '{}'", jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Optional<MappingParameters>> loadMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams params) {
    LOGGER.debug("Trying to load MappingParametersSnapshot by jobExecutionId  '{}' for cache, okapi url: {}, tenantId: {}", jobExecutionId, params.getOkapiUrl(), params.getTenantId());
    MappingMetadataClient client = new MappingMetadataClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());

    return client.getMappingMetadataByJobExecutionId(jobExecutionId)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("MappingParametersSnapshot was loaded by jobExecutionId '{}'", jobExecutionId);
          MappingMetadataDto mappingMetadataDto = httpResponse.bodyAsJson(MappingMetadataDto.class);
          return CompletableFuture.completedFuture(Optional.of(Json.decodeValue(mappingMetadataDto.getMappingParams(), MappingParameters.class)));
        } else if (httpResponse.statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("MappingParametersSnapshot was not found by jobExecutionId '{}'", jobExecutionId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading MappingParametersSnapshot by jobExecutionId: '%s', status code: %s, response message: %s",
            jobExecutionId, httpResponse.statusCode(), httpResponse.bodyAsString());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }
}