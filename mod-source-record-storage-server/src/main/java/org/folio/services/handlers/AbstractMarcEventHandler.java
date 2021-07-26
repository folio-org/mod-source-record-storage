package org.folio.services.handlers;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;

@Component
public abstract class AbstractMarcEventHandler implements EventHandler {
  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC data";

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {

    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    HashMap<String, String> context = dataImportEventPayload.getContext();

    if (context == null || context.isEmpty() || isEmpty(context.get(getEntityType()))){
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
    return (dataImportEventPayload.getContext().containsKey(getEntityType()));
  }

  public abstract String getEntityType();

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

}
