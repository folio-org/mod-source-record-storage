package org.folio.services.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.RecordService;
import org.folio.services.util.TypeConnection;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

/**
 * The abstraction handles the DELETE action
 * The handler:
 * 1. Validates the event payload
 * 2. Retrieves a matched record from the context
 * 3. Updates the existing record with 'deleted' = true
 * 4. If successfully updated - removes a matched record form event payload,
 * and puts external record id to event payload,
 * else completes exceptionally and loggs a cause
 */
public abstract class AbstractDeleteEventHandler implements EventHandler {
  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain required data to modify MARC record";
  private static final String ERROR_WHILE_DELETING_MSG = "Error while deleting MARC record, record is not found";
  protected final TypeConnection typeConnection;
  protected final RecordService recordService;

  public AbstractDeleteEventHandler(RecordService recordService, TypeConnection typeConnection) {
    this.recordService = recordService;
    this.typeConnection = typeConnection;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var payloadContext = payload.getContext();
    if (isNull(payloadContext) || isBlank(payloadContext.get(getRecordKey()))) {
      completeExceptionally(future, new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      return future;
    } else {
      payload.getEventsChain().add(payload.getEventType());
      try {
        handlePayload(payload, future);
      } catch (Throwable exception) {
        completeExceptionally(future, exception);
      }
    }
    return future;
  }

  /* Handles DELETE action  */
  private void handlePayload(DataImportEventPayload payload, CompletableFuture<DataImportEventPayload> future) {
    var record = Json.decodeValue(payload.getContext().get(getRecordKey()), Record.class);
    LOG.info("Handling 'delete' event for the record id = {}", record.getId());
    recordService.updateRecordsState(record.getMatchedId(), RecordState.DELETED, payload.getTenant())
      .onSuccess(ar -> {
        payload.setEventType(getNextEventType());
        payload.getContext().remove(getRecordKey());
        payload.getContext().put(getExternalRecordIdKey(), getExternalRecordId(record.getExternalIdsHolder()));
        future.complete(payload);
      })
      .onFailure(throwable -> completeExceptionally(future, throwable));
  }

  /* Completes exceptionally the given future with the given exception, writing a message in a log */
  private void completeExceptionally(CompletableFuture<DataImportEventPayload> future, Throwable throwable) {
    LOG.error(ERROR_WHILE_DELETING_MSG, throwable);
    future.completeExceptionally(throwable);
  }

  /* Returns the event type that needs to be thrown when the handler is successfully executed */
  protected abstract String getNextEventType();

  /* Returns the string key under which a matched record put into event payload context */
  private String getRecordKey() {
    return "MATCHED_" + typeConnection.getMarcType();
  }

  /* Returns the string key under which an id of external record put into event payload context */
  private String getExternalRecordIdKey() {
    return typeConnection.getExternalType() + "_RECORD_ID";
  }

  /* Returns the external id of the matched record */
  protected abstract String getExternalRecordId(ExternalIdsHolder externalIdsHolder);

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    ProfileSnapshotWrapper currentNode = payload.getCurrentNode();
    if (currentNode != null && ACTION_PROFILE == currentNode.getContentType()) {
      var actionProfile = JsonObject.mapFrom(currentNode.getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == DELETE && (actionProfile.getFolioRecord() == ActionProfile.FolioRecord.valueOf(typeConnection.getMarcType().value()));
    }
    return false;
  }
}
