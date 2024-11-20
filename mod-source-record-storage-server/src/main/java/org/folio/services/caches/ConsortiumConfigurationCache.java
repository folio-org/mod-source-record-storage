package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.services.entities.ConsortiumConfiguration;
import org.folio.services.exceptions.CacheLoadingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cache for {@link ConsortiumConfiguration}
 */
@Component
public class ConsortiumConfigurationCache {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String USER_TENANTS_ENDPOINT = "/user-tenants?limit=1";

  @Value("${srs.consortium-configuration-cache.expiration.time.seconds:3600}")
  private long cacheExpirationTime;
  private AsyncCache<String, Optional<ConsortiumConfiguration>> cache;

  @Autowired
  public ConsortiumConfigurationCache(Vertx vertx) {
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  /**
   * Searches for {@link ConsortiumConfiguration} cache by tenant id
   *
   * @param params okapi connection parameters
   * @return future with optional {@link ConsortiumConfiguration}
   */
  public Future<Optional<ConsortiumConfiguration>> get(OkapiConnectionParams params) {
    try {
      return Future.fromCompletionStage(cache.get(params.getTenantId(), (key, executor) -> loadConsortiumConfiguration(params)));
    } catch (Exception e) {
      LOGGER.warn("get:: Error loading ConsortiumConfiguration by id: '{}'", params.getTenantId(), e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<? extends Optional<ConsortiumConfiguration>> loadConsortiumConfiguration(OkapiConnectionParams params) {
    LOGGER.debug("loadConsortiumConfiguration:: Trying to load consortiumConfiguration by tenantId '{}' for cache, okapi url: {}, tenantId: {}", params.getTenantId(), params.getOkapiUrl(), params.getTenantId());

    return RestUtil.doRequest(params, USER_TENANTS_ENDPOINT, HttpMethod.GET, null)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.getResponse().statusCode() == HttpStatus.SC_OK) {
          JsonArray userTenants = httpResponse.getJson().getJsonArray("userTenants");
          if (userTenants.isEmpty()) {
            LOGGER.debug("loadConsortiumConfiguration:: consortiumConfiguration was not found for tenantId '{}'", params.getTenantId());
            return CompletableFuture.completedFuture(Optional.empty());
          }
          String centralTenantId = userTenants.getJsonObject(0).getString("centralTenantId");
          String consortiumId = userTenants.getJsonObject(0).getString("consortiumId");
          LOGGER.debug("loadConsortiumConfiguration:: Found centralTenantId: '{}' and consortiumId: '{}' was loaded for tenantId: '{}'", centralTenantId, consortiumId, params.getTenantId());
          return CompletableFuture.completedFuture(Optional.of(new ConsortiumConfiguration(centralTenantId, consortiumId)));
        } else {
          String message = String.format("Error loading consortiumConfiguration by tenantId: '%s', status code: %s, response message: %s",
            params.getTenantId(), httpResponse.getResponse().statusCode(), httpResponse.getBody());
          LOGGER.warn("loadConsortiumConfiguration:: {}", message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }
}
