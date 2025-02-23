package org.folio.services.handlers.actions;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.processing.events.services.publisher.KafkaEventPublisher.RECORD_ID_HEADER;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.services.handlers.match.AbstractMarcMatchEventHandler.CENTRAL_TENANT_ID;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.fill035FieldInMarcRecordIfNotExists;
import static org.folio.services.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.services.util.AdditionalFieldsUtil.normalize035;
import static org.folio.services.util.AdditionalFieldsUtil.remove003FieldIfNeeded;
import static org.folio.services.util.AdditionalFieldsUtil.remove035WithActualHrId;
import static org.folio.services.util.AdditionalFieldsUtil.updateLatestTransactionDate;
import static org.folio.services.util.EventHandlingUtil.toOkapiHeaders;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.util.RestUtil;

public abstract class AbstractUpdateModifyEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String USER_ID_HEADER = "userId";
  private static final String PAYLOAD_HAS_NO_DATA_MSG =
    "Failed to handle event payload, cause event payload context does not contain required data to modify MARC record";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'";

  protected RecordService recordService;
  protected SnapshotService snapshotService;
  protected MappingParametersSnapshotCache mappingParametersCache;
  protected Vertx vertx;

  protected AbstractUpdateModifyEventHandler(RecordService recordService, SnapshotService snapshotService,
                                             MappingParametersSnapshotCache mappingParametersCache, Vertx vertx) {
    this.recordService = recordService;
    this.snapshotService = snapshotService;
    this.mappingParametersCache = mappingParametersCache;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    String jobExecutionId = payload.getJobExecutionId();
    String eventType = payload.getEventType();
    String recordId = null;
    LOG.debug("handle:: Start handling event with type '{}' for with jobExecutionId: '{}'", eventType, jobExecutionId);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      var payloadContext = payload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(modifiedEntityType().value()))) {
        LOG.warn(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }
      recordId = payload.getContext().get(RECORD_ID_HEADER);
      LOG.info("handle:: Handling event with type: '{}' for with jobExecutionId: '{}' and recordId: '{}'", eventType, jobExecutionId, recordId);

      MappingProfile mappingProfile = retrieveMappingProfile(payload);
      MappingDetail.MarcMappingOption marcMappingOption = getMarcMappingOption(mappingProfile);
      String hrId = retrieveHrid(payload, marcMappingOption);
      String userId = (String) payload.getAdditionalProperties().get(USER_ID_HEADER);
      Record newRecord = extractRecord(payload, modifiedEntityType().value());
      String incoming001 = getValueFromControlledField(newRecord, HR_ID_FROM_FIELD);
      OkapiConnectionParams okapiParams = getOkapiParams(payload);
      preparePayload(payload);

      LOG.debug("handle:: Load mapping parameters for jobExecutionId: '{}' and recordId: '{}'", jobExecutionId, recordId);
      String finalRecordId = recordId;

      mappingParametersCache.get(payload.getJobExecutionId(), okapiParams)
        .map(mapMappingParametersOrFail(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, payload.getJobExecutionId())))
        .compose(mappingParameters -> modifyRecord(payload, mappingProfile, mappingParameters)
          .compose(v -> prepareModificationResult(payload, marcMappingOption))
          .compose(changedRecord -> {
            LOG.debug("handle:: Start modifying MARC record for jobExecutionId: '{}', recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
            if (isHridFillingNeeded() || isUpdateOption(marcMappingOption)) {
              LOG.debug("handle:: Start filling HRID field for jobExecutionId: '{}', recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
              addControlledFieldToMarcRecord(changedRecord, HR_ID_FROM_FIELD, hrId, true);

              String changed001 = getValueFromControlledField(changedRecord, HR_ID_FROM_FIELD);
              LOG.debug("handle:: Start filling 035 field for jobExecutionId: '{}', and recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
              if (StringUtils.isNotBlank(incoming001) && !incoming001.equals(changed001)) {
                fill035FieldInMarcRecordIfNotExists(changedRecord, incoming001);
              }

              LOG.debug("handle:: Start removing 035 field with actual HRID for jobExecutionId: '{}', recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
              remove035WithActualHrId(changedRecord, hrId);
              remove003FieldIfNeeded(changedRecord, hrId);
            }
            LOG.debug("handle:: Start increasing generation record for jobExecutionId: '{}', recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
            increaseGeneration(changedRecord);

            LOG.debug("handle:: Start setting updatedBy for jobExecutionId: '{}', recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
            setUpdatedBy(changedRecord, userId);

            LOG.debug("handle:: Start updating latest transaction date for jobExecutionId: '{}', recordId: '{}'and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
            updateLatestTransactionDate(changedRecord, mappingParameters);

            LOG.debug("handle:: Start normalizing 035 field for jobExecutionId: '{}', recordId: '{}' and changedRecordId '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
            normalize035(changedRecord);
            payloadContext.put(modifiedEntityType().value(), Json.encode(changedRecord));
            return Future.succeededFuture(changedRecord);
          })
          .compose(changedRecord -> {
            String centralTenantId = payload.getContext().get(CENTRAL_TENANT_ID);
            LOG.debug("handle:: Start saving modified MARC record for jobExecutionId: '{}', recordId: '{}' and changedRecordId: '{}' for centralTenantId: '{}'", jobExecutionId, finalRecordId, changedRecord.getId(), centralTenantId);
            var okapiHeaders = toOkapiHeaders(payload);
            if (centralTenantId != null) {
              okapiHeaders.put(OKAPI_TENANT_HEADER, centralTenantId);
              return snapshotService.copySnapshotToOtherTenant(changedRecord.getSnapshotId(), payload.getTenant(), centralTenantId)
                .compose(snapshot -> recordService.saveRecord(changedRecord, okapiHeaders));
            }
            LOG.debug("handle:: Start saving modified MARC record for jobExecutionId: '{}', recordId: '{}' and changedRecordId: '{}'", jobExecutionId, finalRecordId, changedRecord.getId());
            return recordService.saveRecord(changedRecord, okapiHeaders);
          })
        )
        .onSuccess(savedRecord -> submitSuccessfulEventType(payload, future, marcMappingOption))
        .onFailure(throwable -> {
          LOG.error("handle:: Error while processing for jobExecutionId: {} and recordId: {}", jobExecutionId, finalRecordId, throwable);
          future.completeExceptionally(throwable);
        });
    } catch (Exception e) {
      LOG.error("handle:: Error modifying MARC record for jobExecutionId: {} and recordId: {}", jobExecutionId, recordId, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  protected void submitSuccessfulEventType(DataImportEventPayload payload, CompletableFuture<DataImportEventPayload> future, MappingDetail.MarcMappingOption marcMappingOption) {
    String recordId = payload.getContext().get(RECORD_ID_HEADER);
    String updatedEventType = getUpdateEventType();
    LOG.debug("submitSuccessfulEventType:: Start submitting successful event type '{}' for jobExecutionId: '{}' and recordId: '{}'",
      updatedEventType, payload.getJobExecutionId(), recordId);
      payload.setEventType(updatedEventType);
      future.complete(payload);
  }

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    if (payload.getCurrentNode() != null && ACTION_PROFILE == payload.getCurrentNode().getContentType()) {
      var actionProfile = JsonObject.mapFrom(payload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return isEligibleActionProfile(actionProfile);
    }
    return false;
  }

  protected abstract boolean isHridFillingNeeded();

  protected abstract String getUpdateEventType();

  protected abstract EntityType modifiedEntityType();

  protected MappingDetail.MarcMappingOption getMarcMappingOption(MappingProfile mappingProfile) {
    return mappingProfile.getMappingDetails().getMarcMappingOption();
  }

  private boolean isEligibleActionProfile(ActionProfile actionProfile) {
    return actionProfile.getFolioRecord() == ActionProfile.FolioRecord.valueOf(modifiedEntityType().value())
      && actionProfile.getAction() == UPDATE;
  }

  protected Future<Void> modifyRecord(DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile,
                                      MappingParameters mappingParameters) {

    String jobExecutionId = dataImportEventPayload.getJobExecutionId();
    String recordId = dataImportEventPayload.getContext().get(RECORD_ID_HEADER);
    try {
      LOG.debug("modifyRecord:: Start modifying record for jobExecutionId: '{}' and recordId: '{}'", jobExecutionId, recordId);
      MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
      marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType());
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(dataImportEventPayload);
      return Future.succeededFuture();
    } catch (IOException e) {
      LOG.error("modifyRecord:: Failed to modify record for jobExecutionId: '{}' and recordId: '{}'", jobExecutionId, recordId, e);
      return Future.failedFuture(e);
    }
  }

  private String retrieveHrid(DataImportEventPayload eventPayload, MappingDetail.MarcMappingOption marcMappingOption) {
    String recordAsString = getRecordAsString(eventPayload, marcMappingOption);

    Record recordWithHrid = Json.decodeValue(recordAsString, Record.class);
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

  private Future<Record> prepareModificationResult(DataImportEventPayload payload, MappingDetail.MarcMappingOption marcMappingOption) {
    LOG.debug("prepareModificationResult:: Start preparing modification result for jobExecutionId: '{}'", payload.getJobExecutionId());
    try {
      Record changedRecord = null;
      HashMap<String, String> context = payload.getContext();
      if (isUpdateOption(marcMappingOption)) {
        changedRecord = Json.decodeValue(context.remove(getMatchedMarcKey()), Record.class);
        changedRecord.setSnapshotId(payload.getJobExecutionId());
        changedRecord.setGeneration(null);
        changedRecord.setId(UUID.randomUUID().toString());
        Record incomingRecord = Json.decodeValue(context.get(modifiedEntityType().value()), Record.class);
        changedRecord.setOrder(incomingRecord.getOrder());
        context.put(modifiedEntityType().value(), Json.encode(changedRecord));
      }
      if (changedRecord != null) {
        LOG.debug("prepareModificationResult:: Modification result has been prepared for jobExecutionId: '{}' and recordId: '{}'",
          payload.getJobExecutionId(), changedRecord.getId());
      }
      return Future.succeededFuture(changedRecord);
    } catch (Exception e) {
      LOG.error("prepareModificationResult:: Error preparing modification result for jobExecutionId: '{}'", payload.getJobExecutionId(), e);
      return Future.failedFuture(e);
    }
  }

  private boolean isUpdateOption(MappingDetail.MarcMappingOption marcMappingOption) {
    return marcMappingOption == MappingDetail.MarcMappingOption.UPDATE;
  }

  protected MappingProfile retrieveMappingProfile(DataImportEventPayload dataImportEventPayload) {
    ProfileSnapshotWrapper mappingProfileWrapper = dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0);
    return new JsonObject((Map) mappingProfileWrapper.getContent()).mapTo(MappingProfile.class);
  }

  private void preparePayload(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void increaseGeneration(Record changedRecord) {
    LOG.debug("increaseGeneration:: Start increasing generation for record with id: '{}'", changedRecord.getId());
    var generation = changedRecord.getGeneration();
    if (nonNull(generation)) {
      changedRecord.setGeneration(++generation);
    }
  }

  private void setUpdatedBy(Record changedRecord, String userId) {
    if (changedRecord.getMetadata() != null) {
      changedRecord.getMetadata().setUpdatedByUserId(userId);
    } else {
      changedRecord.withMetadata(new Metadata().withUpdatedByUserId(userId));
    }
  }

  private Function<Optional<MappingParameters>, MappingParameters> mapMappingParametersOrFail(String message) {
    return mappingParameters -> mappingParameters.orElseThrow(() -> new EventProcessingException(message));
  }

  protected Record extractRecord(DataImportEventPayload payload, String key) {
    return Json.decodeValue(payload.getContext().get(key), Record.class);
  }

  protected OkapiConnectionParams getOkapiParams(DataImportEventPayload payload) {
    return RestUtil.retrieveOkapiConnectionParams(payload, vertx);
  }
}
