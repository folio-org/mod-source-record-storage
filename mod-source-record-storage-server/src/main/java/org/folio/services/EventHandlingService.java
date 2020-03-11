package org.folio.services;

import io.vertx.core.Future;

/**
 * Event handling service
 */
public interface EventHandlingService {

  /**
   * Handles specified event content
   *
   * @param eventContent event content to handle
   * @param tenantId     tenant id
   * @return future with true if the event was processed successfully
   */
  Future<Boolean> handle(String eventContent, String tenantId);
}
