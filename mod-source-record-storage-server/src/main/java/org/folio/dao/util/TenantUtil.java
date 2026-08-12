package org.folio.dao.util;

/**
 * The util class helps to work with tenant's data
 */
public class TenantUtil {

  private TenantUtil() {
  }

  public static double calculateDurationSeconds(long startTime) {
    long endTime =  System.nanoTime();
    return (endTime - startTime) / 1_000_000.0 / 1_000;
  }
}
