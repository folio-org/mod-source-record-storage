package org.folio.services.caches;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Configuration of {@link JobProfileSnapshotCache} ({@code srs.profile-snapshot-cache.*} properties).
 *
 * @param expirationTimeSeconds time after last access after which a cached snapshot expires
 * @param notFoundRetryAttempts total number of attempts to load a snapshot when it is not found (HTTP 404), at least 1
 * @param notFoundRetryDelayMs  delay between attempts to load a snapshot that was not found, in milliseconds
 */
@Component
public record JobProfileSnapshotCacheProperties(
  @Value("${srs.profile-snapshot-cache.expiration.time.seconds:3600}") long expirationTimeSeconds,
  @Value("${srs.profile-snapshot-cache.not-found.retry.attempts:3}") int notFoundRetryAttempts,
  @Value("${srs.profile-snapshot-cache.not-found.retry.delay.ms:500}") long notFoundRetryDelayMs) {

  public JobProfileSnapshotCacheProperties {
    notFoundRetryAttempts = Math.max(1, notFoundRetryAttempts);
    notFoundRetryDelayMs = Math.max(1, notFoundRetryDelayMs);
  }
}
