package org.folio.services.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;

@Component
public class MarcAuthorityEventHandler implements EventHandler {
  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_AUTHORITY data";

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    HashMap<String, String> context = dataImportEventPayload.getContext();

    if (context == null || context.isEmpty() || isEmpty(dataImportEventPayload.getContext().get(MARC_AUTHORITY.value()))){
      LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
      future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      return future;
    }
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    future.complete(dataImportEventPayload);
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    return (dataImportEventPayload.getContext().containsKey(MARC_AUTHORITY.value()));
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

}
