package org.folio.services.handlers.actions;

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
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.MappingParametersSnapshotCache;
import org.folio.services.RecordService;
import org.folio.services.util.AdditionalFieldsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

@Component
public class ModifyRecordEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data to modify MARC record";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'";

  public static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private RecordService recordService;
  private MappingParametersSnapshotCache mappingParametersCache;
  private Vertx vertx;

  @Autowired
  public ModifyRecordEventHandler(RecordService recordService, MappingParametersSnapshotCache mappingParametersCache, Vertx vertx) {
    this.recordService = recordService;
    this.mappingParametersCache = mappingParametersCache;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))) {
        LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }
      MappingProfile mappingProfile = retrieveMappingProfile(dataImportEventPayload);
      String hrId = retrieveHrid(dataImportEventPayload, mappingProfile.getMappingDetails().getMarcMappingOption());
      preparePayload(dataImportEventPayload);

      mappingParametersCache.get(dataImportEventPayload.getJobExecutionId(), retrieveOkapiConnectionParams(dataImportEventPayload))
        .compose(parametersOptional -> parametersOptional
          .map(mappingParams -> modifyRecord(dataImportEventPayload, mappingProfile, mappingParams))
          .orElseGet(() -> Future.failedFuture(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, dataImportEventPayload.getJobExecutionId()))))
        .onSuccess(v -> prepareModificationResult(dataImportEventPayload, mappingProfile.getMappingDetails().getMarcMappingOption()))
        .map(v -> Json.decodeValue(payloadContext.get(MARC_BIBLIOGRAPHIC.value()), Record.class))
        .onSuccess(changedRecord -> {
          AdditionalFieldsUtil.addControlledFieldToMarcRecord(changedRecord, AdditionalFieldsUtil.HR_ID_FROM_FIELD, hrId, true);
          AdditionalFieldsUtil.remove003FieldIfNeeded(changedRecord, hrId);
          payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(changedRecord));
        })
        .compose(changedRecord -> recordService.saveRecord(changedRecord, dataImportEventPayload.getTenant()))
        .onComplete(saveAr -> {
          if (saveAr.succeeded()) {
            dataImportEventPayload.setEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value());
            future.complete(dataImportEventPayload);
          } else {
            LOG.error("Error while MARC record modifying", saveAr.cause());
            future.completeExceptionally(saveAr.cause());
          }
        });
    } catch (Exception e) {
      LOG.error("Error modifying MARC record", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private Future<Void> modifyRecord(DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile, MappingParameters mappingParameters) {
    try {
      MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
      marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile);
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(dataImportEventPayload);
      return Future.succeededFuture();
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private String retrieveHrid(DataImportEventPayload eventPayload, MappingDetail.MarcMappingOption marcMappingOption) throws IOException {
    String recordAsString = marcMappingOption == MappingDetail.MarcMappingOption.UPDATE
      ? eventPayload.getContext().get(MATCHED_MARC_BIB_KEY) : eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());

    Record recordWithHrid = OBJECT_MAPPER.readValue(recordAsString, Record.class);
    return AdditionalFieldsUtil.getValueFromControlledField(recordWithHrid, AdditionalFieldsUtil.HR_ID_FROM_FIELD);
  }

  private void prepareModificationResult(DataImportEventPayload dataImportEventPayload, MappingDetail.MarcMappingOption marcMappingOption) {
    HashMap<String, String> context = dataImportEventPayload.getContext();
    if (marcMappingOption == MappingDetail.MarcMappingOption.UPDATE) {
      Record changedRecord = Json.decodeValue(context.remove(MATCHED_MARC_BIB_KEY), Record.class);
      changedRecord.setSnapshotId(dataImportEventPayload.getJobExecutionId());
      changedRecord.setGeneration(null);
      changedRecord.setId(UUID.randomUUID().toString());
      context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(changedRecord));
    }
  }

  private MappingProfile retrieveMappingProfile(DataImportEventPayload dataImportEventPayload) {
    ProfileSnapshotWrapper mappingProfileWrapper = dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0);
    return new JsonObject((Map) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
  }

  private void preparePayload(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getFolioRecord() == ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC
        && (actionProfile.getAction() == MODIFY || actionProfile.getAction() == UPDATE);
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
  }

  private OkapiConnectionParams retrieveOkapiConnectionParams(DataImportEventPayload eventPayload) {
    return new OkapiConnectionParams(Map.of(
      RestUtil.OKAPI_URL_HEADER, eventPayload.getOkapiUrl(),
      RestUtil.OKAPI_TENANT_HEADER, eventPayload.getTenant(),
      RestUtil.OKAPI_TOKEN_HEADER, eventPayload.getToken()
    ), this.vertx);
  }
}
