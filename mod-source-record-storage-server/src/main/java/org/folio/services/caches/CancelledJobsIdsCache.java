package org.folio.services.caches;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * An in-memory cache that stores IDs of cancelled import jobs.
 */
@Component
public class CancelledJobsIdsCache {

  private final Cache<String, Boolean> cache;

  public CancelledJobsIdsCache(
    @Value("${srs.cancelled-job-cache.expiration.time.minutes:1440}") long cacheExpirationTimeMins) {
    this.cache = Caffeine.newBuilder()
      .expireAfterWrite(cacheExpirationTimeMins, TimeUnit.MINUTES)
      .build();
  }

  /**
   * Puts the specified {@code jobId} into the cache.
   *
   * @param jobId import job id to put into the cache
   * @throws NullPointerException if the specified {@code jobId} is null
   */
  public void put(String jobId) {
    cache.put(jobId, Boolean.TRUE);
  }

  /**
   * Checks if the cache contains the specified {@code jobId}.
   *
   * @param jobId import job id to check
   * @return {@code true} if the cache contains the {@code jobId}, {@code false} otherwise
   * @throws NullPointerException if the specified {@code jobId} is null
   */
  public boolean contains(String jobId) {
    return cache.asMap().containsKey(jobId);
  }

}
