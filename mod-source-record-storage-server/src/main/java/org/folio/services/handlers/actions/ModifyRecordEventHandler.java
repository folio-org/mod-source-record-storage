package org.folio.services.handlers.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

@Component
public class ModifyRecordEventHandler implements EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ModifyRecordEventHandler.class);
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data to modify MARC record";

  public static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  private RecordService recordService;

  @Autowired
  public ModifyRecordEventHandler(RecordService recordService) {
    this.recordService = recordService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))
        || isBlank(payloadContext.get(MAPPING_PARAMS_KEY))) {
        LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }
      MappingProfile mappingProfile = retrieveMappingProfile(dataImportEventPayload);
      preparePayload(dataImportEventPayload);

      MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
      marcRecordModifier.initialize(dataImportEventPayload, mappingProfile);
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(dataImportEventPayload);
      prepareModificationResult(dataImportEventPayload, mappingProfile.getMappingDetails().getMarcMappingOption());

      Record changedRecord = new ObjectMapper().readValue(payloadContext.get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      recordService.saveRecord(changedRecord, dataImportEventPayload.getTenant())
        .onComplete(saveAr -> {
          if (saveAr.succeeded()) {
            dataImportEventPayload.setEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value());
            future.complete(dataImportEventPayload);
          } else {
            LOG.error("Error saving modified MARC record", saveAr.cause());
            future.completeExceptionally(saveAr.cause());
          }
        });
    } catch (Exception e) {
      LOG.error("Error modifying MARC record", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private void prepareModificationResult(DataImportEventPayload dataImportEventPayload, MappingDetail.MarcMappingOption marcMappingOption) {
    HashMap<String, String> context = dataImportEventPayload.getContext();
    if (marcMappingOption == MappingDetail.MarcMappingOption.UPDATE) {
      context.put(MARC_BIBLIOGRAPHIC.value(), context.remove(MATCHED_MARC_BIB_KEY));
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
      return actionProfile.getAction() == ActionProfile.Action.MODIFY && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
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
}
