package org.folio.services;

import io.vertx.core.Vertx;

/* The service is intended for cleaning activities */
public interface RecordCleanupService {

  /**
   * Performs an initialization of the service
   *
   * @param vertx     Vertx object
   * @param tenantId  tenant id
   * @return long timer id
   */
  long initialize(Vertx vertx, String tenantId);
}
