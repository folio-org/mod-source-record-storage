package org.folio.services;

import io.vertx.core.Future;

/**
 * Event handling service
 */
public interface EventHandlingService {

  /**
   * Handles specified event content for create
   *
   * @param eventContent event content to handle
   * @param tenantId     tenant id
   * @return future with true if the event was processed successfully
   */
  Future<Boolean> handleCreate(String eventContent, String tenantId);

  /**
   * Handles specified event content for update
   *
   * @param eventContent event content to handle
   * @param tenantId     tenant id
   * @return future with true if the event was processed successfully
   */
  Future<Boolean> handleUpdate(String eventContent, String tenantId);
}
