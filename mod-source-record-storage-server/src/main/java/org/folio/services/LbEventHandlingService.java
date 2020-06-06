package org.folio.services;

import org.folio.rest.util.OkapiConnectionParams;

import io.vertx.core.Future;

/**
 * Event handling service
 */
public interface LbEventHandlingService {

  /**
   * Handles specified event content for event
   *
   * @param eventContent event content to handle
   * @param params       Okapi connection params including Okapi URI, tenant id, and token
   * @return future with true if the event was processed successfully
   */
  Future<Boolean> handleEvent(String eventContent, OkapiConnectionParams params);

}
