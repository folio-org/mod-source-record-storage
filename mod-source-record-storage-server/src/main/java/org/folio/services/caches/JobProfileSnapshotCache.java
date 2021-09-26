package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.DataImportProfilesClient;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.exceptions.CacheLoadingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class JobProfileSnapshotCache {

  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${srs.profile-snapshot-cache.expiration.time.seconds:3600}")
  private long cacheExpirationTime;
  private AsyncCache<String, Optional<ProfileSnapshotWrapper>> cache;

  @Autowired
  public JobProfileSnapshotCache(Vertx vertx) {
    cache = Caffeine.newBuilder()
      .expireAfterAccess(cacheExpirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  public Future<Optional<ProfileSnapshotWrapper>> get(String profileSnapshotId, OkapiConnectionParams params) {
    try {
      return Future.fromCompletionStage(cache.get(profileSnapshotId, (key, executor) -> loadJobProfileSnapshot(key, params)));
    } catch (Exception e) {
      LOGGER.warn("Error loading ProfileSnapshotWrapper by id: '{}'", profileSnapshotId, e);
      return Future.failedFuture(e);
    }
  }

  private CompletableFuture<Optional<ProfileSnapshotWrapper>> loadJobProfileSnapshot(String profileSnapshotId, OkapiConnectionParams params) {
    LOGGER.debug("Trying to load jobProfileSnapshot by id  '{}' for cache, okapi url: {}, tenantId: {}", profileSnapshotId, params.getOkapiUrl(), params.getTenantId());
    DataImportProfilesClient client = new DataImportProfilesClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());

    return client.getDataImportProfilesJobProfileSnapshotsById(profileSnapshotId)
      .toCompletionStage()
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == HttpStatus.SC_OK) {
          LOGGER.info("JobProfileSnapshot was loaded by id '{}'", profileSnapshotId);
          return CompletableFuture.completedFuture(Optional.of(httpResponse.bodyAsJson(ProfileSnapshotWrapper.class)));
        } else if (httpResponse.statusCode() == HttpStatus.SC_NOT_FOUND) {
          LOGGER.warn("JobProfileSnapshot was not found by id '{}'", profileSnapshotId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format("Error loading jobProfileSnapshot by id: '%s', status code: %s, response message: %s",
            profileSnapshotId, httpResponse.statusCode(), httpResponse.bodyAsString());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new CacheLoadingException(message));
        }
      });
  }

}
