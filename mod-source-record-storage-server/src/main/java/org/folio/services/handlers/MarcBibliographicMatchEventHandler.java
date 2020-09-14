package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

import java.util.concurrent.CompletableFuture;

import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.matching.MatchingManager;

import io.vertx.core.json.JsonObject;

public class MarcBibliographicMatchEventHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());

    MatchingManager.match(dataImportEventPayload)
      .whenComplete((matched, throwable) -> {
        if (throwable != null) {
          future.completeExceptionally(throwable);
        } else {
          if (Boolean.TRUE.equals(matched)) {
            dataImportEventPayload.setEventType("DI_INVENTORY_MARC_BIBLIOGRAPHIC_MATCHED");
          } else {
            dataImportEventPayload.setEventType("DI_INVENTORY_MARC_BIBLIOGRAPHIC_NOT_MATCHED");
          }
          future.complete(dataImportEventPayload);
        }
      });
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MatchProfile matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return matchProfile.getIncomingRecordType() == MARC_BIBLIOGRAPHIC;
    }
    return false;
  }
}
