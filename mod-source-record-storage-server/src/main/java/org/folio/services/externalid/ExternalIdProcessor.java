package org.folio.services.externalid;

import java.util.Optional;

import org.folio.rest.jaxrs.model.SourceRecord;

import io.vertx.core.Future;

/**
 * Interface for processing search via different externalId
 */
public interface ExternalIdProcessor {

  /**
   * Call recordDao with specific parameters for needed external type
   * @param id - id
   * @param tenantId - tenant id
   * @return future with optional source record which was found
   */
  Future<Optional<SourceRecord>> process(String id, String tenantId);
}
