package org.folio.services.caches;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class CancelledJobsIdsCacheTest {

  private static final long CACHE_EXPIRATION_TIME_MINS = 5;

  private CancelledJobsIdsCache cache;

  @Before
  public void setUp() {
    cache = new CancelledJobsIdsCache(CACHE_EXPIRATION_TIME_MINS);
  }

  @Test
  public void shouldIdAddToCache() {
    String jobId = UUID.randomUUID().toString();
    cache.put(jobId);
    assertTrue(cache.contains(jobId));
  }

  @Test
  public void shouldReturnFalseForNonExistentId() {
    String jobId = UUID.randomUUID().toString();
    assertFalse(cache.contains(jobId));
  }

  @Test
  public void shouldThrowExceptionIfJobIdIsNull() {
    assertThrows(NullPointerException.class, () -> cache.contains(null));
  }

}
