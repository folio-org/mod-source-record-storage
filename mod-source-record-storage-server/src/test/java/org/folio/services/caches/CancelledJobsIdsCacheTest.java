package org.folio.services.caches;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CancelledJobsIdsCacheTest {

  private static final long CACHE_EXPIRATION_TIME_MINS = 5;

  private CancelledJobsIdsCache cache;

  @BeforeEach
  void setUp() {
    cache = new CancelledJobsIdsCache(CACHE_EXPIRATION_TIME_MINS);
  }

  @Test
  void shouldIdAddToCache() {
    String jobId = UUID.randomUUID().toString();
    cache.put(jobId);
    assertTrue(cache.contains(jobId));
  }

  @Test
  void shouldReturnFalseForNonExistentId() {
    String jobId = UUID.randomUUID().toString();
    assertFalse(cache.contains(jobId));
  }

  @Test
  void shouldThrowExceptionIfJobIdIsNull() {
    assertThrows(NullPointerException.class, () -> cache.contains(null));
  }

}
