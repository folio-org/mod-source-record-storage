package org.folio.services.externalid;

import java.util.Optional;

import org.folio.rest.jaxrs.model.SourceRecord;

import io.vertx.core.Future;

public interface ExternalIdProcessor {

  Future<Optional<SourceRecord>> process(String id, String tenantId);
}
