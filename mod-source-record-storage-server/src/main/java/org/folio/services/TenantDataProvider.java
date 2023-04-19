package org.folio.services;

import io.vertx.core.Future;

import java.util.List;

/**
 * Provides information regarding registered tenants in the system.
 */
public interface TenantDataProvider {
  /**
   * Gets all module tenants.
   *
   * @return tenant ids
   */
  Future<List<String>> getModuleTenants();
}
