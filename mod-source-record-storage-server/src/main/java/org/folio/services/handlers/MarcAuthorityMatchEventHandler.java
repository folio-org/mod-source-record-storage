package org.folio.services.handlers;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Handler for MARC-MARC matching/not-matching MARC-record by specific fields.
 */
@Component
public class MarcAuthorityMatchEventHandler implements EventHandler {
  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    return null;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    return false;
  }
}
