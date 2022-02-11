package org.folio.services.handlers.actions;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.services.util.AdditionalFieldsUtil.remove003FieldIfNeeded;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.util.RestUtil;

public abstract class AbstractUpdateModifyEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG =
    "Failed to handle event payload, cause event payload context does not contain required data to modify MARC record";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG =
    "MappingParameters snapshot was not found by jobExecutionId '%s'";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected RecordService recordService;
  protected MappingParametersSnapshotCache mappingParametersCache;
  protected Vertx vertx;

  public AbstractUpdateModifyEventHandler(
    RecordService recordService, MappingParametersSnapshotCache mappingParametersCache, Vertx vertx) {
    this.recordService = recordService;
    this.mappingParametersCache = mappingParametersCache;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      var payloadContext = payload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(modifiedEntityType().value()))) {
        LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }

      MappingProfile mappingProfile = retrieveMappingProfile(payload);
      String hrId = retrieveHrid(payload, getMarcMappingOption(mappingProfile));
      preparePayload(payload);

      mappingParametersCache.get(payload.getJobExecutionId(), RestUtil.retrieveOkapiConnectionParams(payload, vertx))
        .compose(parametersOptional -> parametersOptional
          .map(mappingParams -> modifyRecord(payload, mappingProfile, mappingParams))
          .orElseGet(
            () -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, payload.getJobExecutionId()))))
        .onSuccess(v -> prepareModificationResult(payload, getMarcMappingOption(mappingProfile)))
        .map(v -> Json.decodeValue(payloadContext.get(modifiedEntityType().value()), Record.class))
        .onSuccess(changedRecord -> {
          addControlledFieldToMarcRecord(changedRecord, HR_ID_FROM_FIELD, hrId, true);
          remove003FieldIfNeeded(changedRecord, hrId);
          increaseGeneration(changedRecord);
        })
        .compose(changedRecord -> recordService.saveRecord(changedRecord, payload.getTenant()))
        .onSuccess(savedRecord -> {
          payloadContext.put(modifiedEntityType().value(), Json.encode(savedRecord));
          payload.setEventType(getNextEventType());
          future.complete(payload);
        })
        .onFailure(throwable -> {
          LOG.error("Error while MARC record modifying", throwable);
          future.completeExceptionally(throwable);
        });
    } catch (Exception e) {
      LOG.error("Error modifying MARC record", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    if (payload.getCurrentNode() != null && ACTION_PROFILE == payload.getCurrentNode().getContentType()) {
      var actionProfile = JsonObject.mapFrom(payload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return isEligibleActionProfile(actionProfile);
    }
    return false;
  }

  protected abstract String getNextEventType();

  protected abstract EntityType modifiedEntityType();

  private MappingDetail.MarcMappingOption getMarcMappingOption(MappingProfile mappingProfile) {
    return mappingProfile.getMappingDetails().getMarcMappingOption();
  }

  private boolean isEligibleActionProfile(ActionProfile actionProfile) {
    return actionProfile.getFolioRecord() == ActionProfile.FolioRecord.valueOf(modifiedEntityType().value())
      && (actionProfile.getAction() == MODIFY || actionProfile.getAction() == UPDATE);
  }

  private Future<Void> modifyRecord(DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile,
                                    MappingParameters mappingParameters) {
    try {
      MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
      marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType());
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(dataImportEventPayload);
      return Future.succeededFuture();
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private String retrieveHrid(DataImportEventPayload eventPayload, MappingDetail.MarcMappingOption marcMappingOption)
    throws IOException {
    String recordAsString = getRecordAsString(eventPayload, marcMappingOption);

    Record recordWithHrid = OBJECT_MAPPER.readValue(recordAsString, Record.class);
    return getValueFromControlledField(recordWithHrid, HR_ID_FROM_FIELD);
  }

  private String getRecordAsString(DataImportEventPayload eventPayload, MappingDetail.MarcMappingOption marcMappingOption) {
    return isUpdateOption(marcMappingOption)
      ? eventPayload.getContext().get(getMatchedMarcKey())
      : eventPayload.getContext().get(modifiedEntityType().value());
  }

  private String getMatchedMarcKey() {
    return "MATCHED_" + modifiedEntityType();
  }

  private void prepareModificationResult(DataImportEventPayload payload, MappingDetail.MarcMappingOption marcMappingOption) {
    HashMap<String, String> context = payload.getContext();
    if (isUpdateOption(marcMappingOption)) {
      Record changedRecord = Json.decodeValue(context.remove(getMatchedMarcKey()), Record.class);
      changedRecord.setSnapshotId(payload.getJobExecutionId());
      changedRecord.setGeneration(null);
      changedRecord.setId(UUID.randomUUID().toString());
      context.put(modifiedEntityType().value(), Json.encode(changedRecord));
    }
  }

  private boolean isUpdateOption(MappingDetail.MarcMappingOption marcMappingOption) {
    return marcMappingOption == MappingDetail.MarcMappingOption.UPDATE;
  }

  private MappingProfile retrieveMappingProfile(DataImportEventPayload dataImportEventPayload) {
    ProfileSnapshotWrapper mappingProfileWrapper = dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0);
    return new JsonObject((Map) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
  }

  private void preparePayload(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void increaseGeneration(Record changedRecord) {
    var generation = changedRecord.getGeneration();
    changedRecord.setGeneration(isNull(generation) ? 1 : ++generation);
  }
}
