package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.exceptions.CacheLoadingException;
import org.folio.services.exceptions.ProfileSnapshotNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cache of job profile snapshots. Only successfully loaded snapshots are retained: a snapshot that is
 * not found (HTTP 404, possibly transient) completes the loading exceptionally and is therefore evicted
 * by the cache (and removed eagerly, so a miss is never observed by a later call), after a configurable
 * number of retries.
 */
@Component
public class JobProfileSnapshotCache {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String PROFILE_SNAPSHOT_URL = "/data-import-profiles/jobProfileSnapshots/";

  private final Vertx vertx;
  private final JobProfileSnapshotCacheProperties properties;
  private final AsyncCache<String, ProfileSnapshotWrapper> cache;

  @Autowired
  public JobProfileSnapshotCache(Vertx vertx, JobProfileSnapshotCacheProperties properties) {
    this.vertx = vertx;
    this.properties = properties;
    this.cache = Caffeine.newBuilder()
      .expireAfterAccess(properties.expirationTimeSeconds(), TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  /**
   * Gets the snapshot from the cache, loading it if absent.
   *
   * @return succeeded future with the snapshot, or a failed future with
   *   {@link ProfileSnapshotNotFoundException} if it does not exist, or {@link CacheLoadingException} on other errors
   */
  public Future<ProfileSnapshotWrapper> get(String profileSnapshotId, ConnectionParams params) {
    try {
      CompletableFuture<ProfileSnapshotWrapper> cached =
        cache.get(profileSnapshotId, (key, executor) -> load(key, params));
      return Future.fromCompletionStage(cached)
        .onFailure(e -> cache.asMap().remove(profileSnapshotId, cached));
    } catch (Exception e) {
      LOGGER.warn("get:: Error loading ProfileSnapshotWrapper by id: '{}'", profileSnapshotId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<ProfileSnapshotWrapper> load(String profileSnapshotId, ConnectionParams params) {
    LOGGER.debug("load:: Trying to load jobProfileSnapshot by id '{}' for cache, okapi url: {}, tenantId: {}",
      profileSnapshotId, params.getConnectionUrl(), params.getTenantId());
    return loadWithRetry(profileSnapshotId, params, 1).toCompletionStage().toCompletableFuture();
  }

  private Future<ProfileSnapshotWrapper> loadWithRetry(String profileSnapshotId, ConnectionParams params, int attempt) {
    return fetch(profileSnapshotId, params)
      .recover(error -> {
        if (!(error instanceof ProfileSnapshotNotFoundException)) {
          return Future.failedFuture(error);
        }
        if (attempt >= properties.notFoundRetryAttempts()) {
          LOGGER.warn("loadWithRetry:: JobProfileSnapshot '{}' was not found after {} attempt(s), giving up",
            profileSnapshotId, attempt);
          return Future.failedFuture(error);
        }
        LOGGER.warn("loadWithRetry:: JobProfileSnapshot '{}' was not found, attempt {} of {}, retrying in {} ms",
          profileSnapshotId, attempt, properties.notFoundRetryAttempts(), properties.notFoundRetryDelayMs());
        return delay().compose(v -> loadWithRetry(profileSnapshotId, params, attempt + 1));
      });
  }

  private Future<Void> delay() {
    Promise<Void> promise = Promise.promise();
    vertx.setTimer(properties.notFoundRetryDelayMs(), timerId -> promise.complete());
    return promise.future();
  }

  private Future<ProfileSnapshotWrapper> fetch(String profileSnapshotId, ConnectionParams params) {
    return RestUtil.doRequestWithSystemUser(params, PROFILE_SNAPSHOT_URL + profileSnapshotId, HttpMethod.GET, null)
      .compose(httpResponse -> {
        int statusCode = httpResponse.getResponse().statusCode();
        if (statusCode == HttpStatus.HTTP_OK.toInt()) {
          LOGGER.info("fetch:: JobProfileSnapshot was loaded by id '{}'", profileSnapshotId);
          return Future.succeededFuture(Json.decodeValue(httpResponse.getJson().encode(), ProfileSnapshotWrapper.class));
        }
        if (statusCode == HttpStatus.HTTP_NOT_FOUND.toInt()) {
          return Future.failedFuture(new ProfileSnapshotNotFoundException(
            String.format("Job profile snapshot with id '%s' does not exist", profileSnapshotId)));
        }
        String message = String.format("Error loading jobProfileSnapshot by id: '%s', status code: %s, response message: %s",
          profileSnapshotId, statusCode, httpResponse.getBody());
        LOGGER.warn("fetch:: {}", message);
        return Future.failedFuture(new CacheLoadingException(message));
      });
  }
}
