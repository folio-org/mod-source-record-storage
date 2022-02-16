package org.folio.services.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.util.TypeConnection;

import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

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
      var record = Json.decodeValue(payloadContext.get(getRecordKey()), Record.class);
      deleteRecord(record, payload.getTenant())
        .onSuccess(isDeleted -> {
          if (isDeleted) {
            payload.setEventType(getNextEventType());
            payload.getContext().remove(getRecordKey());
            payload.getContext().put(getRecordIdKey(), record.getId());
            future.complete(payload);
          } else {
            completeExceptionally(future, new EventProcessingException(ERROR_WHILE_DELETING_MSG));
          }
        })
        .onFailure(throwable -> completeExceptionally(future, throwable));
    }
    return future;
  }

  /* Deletes a record. Returns Future<Boolean> that means record deleted or not */
  protected abstract Future<Boolean> deleteRecord(Record record, String tenantId);

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

  /* Returns the string key under which an id of deleted record put into event payload context */
  private String getRecordIdKey () {
    return typeConnection.getMarcType() + "_ID";
  }

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
