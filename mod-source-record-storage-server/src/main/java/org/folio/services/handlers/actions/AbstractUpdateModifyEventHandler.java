package org.folio.services.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.MappingProfile;
import org.folio.client.support.Context;
import org.folio.client.InstanceLinkClient;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcBibRecordModifier;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.util.RestUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.fill035FieldInMarcRecordIfNotExists;
import static org.folio.services.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.services.util.AdditionalFieldsUtil.isSubfieldExist;
import static org.folio.services.util.AdditionalFieldsUtil.remove003FieldIfNeeded;
import static org.folio.services.util.AdditionalFieldsUtil.remove035WithActualHrId;

public abstract class AbstractUpdateModifyEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String USER_ID_HEADER = "userId";
  private static final char SUB_FIELD_9 = '9';
  private static final String PAYLOAD_HAS_NO_DATA_MSG =
    "Failed to handle event payload, cause event payload context does not contain required data to modify MARC record";
  private static final String MAPPING_PARAMETERS_NOT_FOUND_MSG = "MappingParameters snapshot was not found by jobExecutionId '%s'";
  private static final String RECORD_NOT_FOUND_MSG = "Record was not found by recordId '%s'";
  protected RecordService recordService;
  protected MappingParametersSnapshotCache mappingParametersCache;
  protected Vertx vertx;
  protected InstanceLinkClient instanceLinkClient;

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
        LOG.warn(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }

      MappingProfile mappingProfile = retrieveMappingProfile(payload);
      MappingDetail.MarcMappingOption marcMappingOption = getMarcMappingOption(mappingProfile);
      String hrId = retrieveHrid(payload, marcMappingOption);
      String userId = (String) payload.getAdditionalProperties().get(USER_ID_HEADER);
      Record newRecord = Json.decodeValue(payloadContext.get(modifiedEntityType().value()), Record.class);
      String incoming001 = getValueFromControlledField(newRecord, HR_ID_FROM_FIELD);
      String instanceId = newRecord.getExternalIdsHolder().getHoldingsId();
      preparePayload(payload);

      mappingParametersCache.get(payload.getJobExecutionId(), RestUtil.retrieveOkapiConnectionParams(payload, vertx))
        .map(mapMappingParametersOrFail(format(MAPPING_PARAMETERS_NOT_FOUND_MSG, payload.getJobExecutionId())))
        .compose(mappingParameters -> (modifiedEntityType() != MARC_BIBLIOGRAPHIC) ?
          modifyRecord(payload, mappingProfile, mappingParameters) :
          resolveModifyRecord(newRecord, payload, userId, mappingProfile, mappingParameters, instanceId))
        .onSuccess(v -> prepareModificationResult(payload, marcMappingOption))
        .map(v -> Json.decodeValue(payloadContext.get(modifiedEntityType().value()), Record.class))
        .onSuccess(changedRecord -> {
          if (isHridFillingNeeded() || isUpdateOption(marcMappingOption)) {
            addControlledFieldToMarcRecord(changedRecord, HR_ID_FROM_FIELD, hrId, true);

            String changed001 = getValueFromControlledField(changedRecord, HR_ID_FROM_FIELD);
            if (StringUtils.isNotBlank(incoming001) && !incoming001.equals(changed001)) {
              fill035FieldInMarcRecordIfNotExists(changedRecord, incoming001);
            }

            remove035WithActualHrId(changedRecord, hrId);
            remove003FieldIfNeeded(changedRecord, hrId);
          }

          increaseGeneration(changedRecord);
          setUpdatedBy(changedRecord, userId);
          payloadContext.put(modifiedEntityType().value(), Json.encode(changedRecord));
        })
        .compose(changedRecord -> recordService.saveRecord(changedRecord, payload.getTenant()))
        .onSuccess(savedRecord -> {
          payload.setEventType(getNextEventType());
          future.complete(payload);
        })
        .onFailure(throwable -> {
          LOG.warn("handle:: Error while MARC record modifying", throwable);
          future.completeExceptionally(throwable);
        });
    } catch (Exception e) {
      LOG.warn("handle:: Error modifying MARC record", e);
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

  protected abstract boolean isHridFillingNeeded();

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

  private void modifyBibRecord(DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile,
    MappingParameters mappingParameters, InstanceLinkDtoCollection links, Promise<InstanceLinkDtoCollection> promise) {
    try {
      MarcBibRecordModifier marcRecordModifier = new MarcBibRecordModifier();
      marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType(), links);
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(dataImportEventPayload);
      promise.complete(new InstanceLinkDtoCollection().withLinks(marcRecordModifier.getBibAuthorityLinksKept()));
    } catch (IOException e) {
      promise.fail(e);
    }
  }

  private Future<Void> resolveModifyRecord(Record newRecord, DataImportEventPayload payload, String userId,
    MappingProfile mappingProfile, MappingParameters mappingParameters, String instanceId) {
    Context context = new Context(payload.getTenant(), payload.getToken(), payload.getOkapiUrl(), userId);
    return recordService.getRecordById(newRecord.getId(), payload.getTenant())
      .map(optionalRecord -> optionalRecord.orElseThrow(
        () -> new EventProcessingException(format(RECORD_NOT_FOUND_MSG, newRecord.getId()))))
      .compose(oldRecord -> {
        Promise<InstanceLinkDtoCollection> promise = Promise.promise();
        if (isSubfieldExist(oldRecord, SUB_FIELD_9)) {
          instanceLinkClient.getLinksByInstanceId(instanceId, context)
            .whenComplete((instanceLinkDtoCollection, throwable) -> {
              if (throwable != null) {
                LOG.error(throwable.getMessage());
                promise.fail(throwable);
              } else {
                modifyBibRecord(payload, mappingProfile, mappingParameters, instanceLinkDtoCollection.get(), promise);
              }
            });
        } else {
          try {
            MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
            marcRecordModifier.initialize(payload, mappingParameters, mappingProfile, modifiedEntityType());
            marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
            marcRecordModifier.getResult(payload);
            promise.complete(new InstanceLinkDtoCollection().withLinks(List.of()));
          } catch (IOException e) {
            promise.fail(e);
          }
        }
        return promise.future();
      })
      .compose(instanceLinkDtoCollection -> {
        Promise<Void> promise = Promise.promise();
        if (instanceLinkDtoCollection != null && !instanceLinkDtoCollection.getLinks().isEmpty()) {
          instanceLinkClient.updateInstanceLinks(instanceId, instanceLinkDtoCollection, context)
            .whenComplete((v, throwable) -> {
              if (throwable != null) {
                promise.complete();
              } else {
                promise.fail(throwable);
              }
            });
        } else {
          promise.complete();
        }
        return promise.future();
      });
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

  private void prepareModificationResult(DataImportEventPayload payload, MappingDetail.MarcMappingOption marcMappingOption) {
    HashMap<String, String> context = payload.getContext();
    if (isUpdateOption(marcMappingOption)) {
      Record changedRecord = Json.decodeValue(context.remove(getMatchedMarcKey()), Record.class);
      changedRecord.setSnapshotId(payload.getJobExecutionId());
      changedRecord.setGeneration(null);
      changedRecord.setId(UUID.randomUUID().toString());
      Record incomingRecord = Json.decodeValue(context.get(modifiedEntityType().value()), Record.class);
      changedRecord.setOrder(incomingRecord.getOrder());
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
}
