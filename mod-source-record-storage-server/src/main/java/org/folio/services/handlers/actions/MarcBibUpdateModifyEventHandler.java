package org.folio.services.handlers.actions;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.services.util.AdditionalFieldsUtil.isSubfieldExist;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.MappingProfile;
import org.folio.client.InstanceLinkClient;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcBibRecordModifier;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.folio.rest.jaxrs.model.EntityType;
import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;

@Component
public class MarcBibUpdateModifyEventHandler extends AbstractUpdateModifyEventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String RECORD_NOT_FOUND_MSG = "Record was not found by recordId '%s'";
  private static final char SUB_FIELD_9 = '9';

  @Autowired
  public MarcBibUpdateModifyEventHandler(RecordService recordService, MappingParametersSnapshotCache mappingParametersCache,
                                         Vertx vertx, InstanceLinkClient instanceLinkClient) {
    super(recordService, mappingParametersCache, vertx);
    this.instanceLinkClient = instanceLinkClient;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  protected boolean isHridFillingNeeded() {
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected String getNextEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED.value();
  }

  @Override
  protected EntityType modifiedEntityType() {
    return MARC_BIBLIOGRAPHIC;
  }

  @Override
  protected Future<Void> modifyRecord(Record newRecord, DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile,
    MappingParameters mappingParameters, String instanceId, OkapiConnectionParams okapiParams){
    return recordService.getRecordById(newRecord.getId(), dataImportEventPayload.getTenant())
      .map(optionalRecord -> optionalRecord.orElseThrow(() -> new EventProcessingException(format(RECORD_NOT_FOUND_MSG, newRecord.getId()))))
      .map(oldRecord -> isSubfieldExist(oldRecord, SUB_FIELD_9))
      .compose(subfieldExist -> Boolean.TRUE.equals(subfieldExist) ? loadInstanceLink(instanceId, okapiParams) : Future.succeededFuture(Optional.empty()))
      .compose(linksOptional -> modifyMarcBibRecord(dataImportEventPayload, mappingProfile, mappingParameters, linksOptional))
      .compose(linksOptional -> updateInstanceLinks(instanceId, linksOptional, okapiParams));
  }

  private Future<Optional<InstanceLinkDtoCollection>> loadInstanceLink(String instanceId, OkapiConnectionParams okapiParams) {
    Promise<Optional<InstanceLinkDtoCollection>> promise = Promise.promise();
    instanceLinkClient.getLinksByInstanceId(instanceId, okapiParams)
      .whenComplete((instanceLinkDtoCollection, throwable) -> {
        if (throwable != null) {
          LOG.error(throwable.getMessage());
          promise.fail(throwable);
        } else {
          promise.complete(instanceLinkDtoCollection);
        }
      });
    return promise.future();
  }

  private Future<Optional<InstanceLinkDtoCollection>> modifyMarcBibRecord(DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile,
    MappingParameters mappingParameters, Optional<InstanceLinkDtoCollection> linksOptional) {
    Promise<Optional<InstanceLinkDtoCollection>> promise = Promise.promise();
    try {
      if (linksOptional.isPresent()) {
        MarcBibRecordModifier marcRecordModifier = new MarcBibRecordModifier();
        marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType(), linksOptional.get());
        marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
        marcRecordModifier.getResult(dataImportEventPayload);
        if (isLinksTheSame(linksOptional.get(), marcRecordModifier.getBibAuthorityLinksKept())) {
          promise.complete(Optional.empty());
        } else {
          promise.complete(Optional.of(new InstanceLinkDtoCollection().withLinks(marcRecordModifier.getBibAuthorityLinksKept())));
        }
      } else {
        MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
        marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType());
        marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
        marcRecordModifier.getResult(dataImportEventPayload);
        promise.complete(Optional.empty());
      }
    } catch (IOException e) {
      promise.fail(e);
    }
    return promise.future();
  }

  private boolean isLinksTheSame(InstanceLinkDtoCollection links, List<Link> bibAuthorityLinksKept) {
    if (links.getLinks().size() != bibAuthorityLinksKept.size()) {
      return false;
    }
    for (Link link : links.getLinks()) {
      if (!bibAuthorityLinksKept.contains(link)) {
        return false;
      }
    }
    return true;
  }

  private Future<Void> updateInstanceLinks(String instanceId, Optional<InstanceLinkDtoCollection> linkOptional,
                                           OkapiConnectionParams okapiParams) {
    Promise<Void> promise = Promise.promise();
    if (linkOptional.isPresent()) {
      instanceLinkClient.updateInstanceLinks(instanceId, linkOptional.get(), okapiParams)
        .whenComplete((v, throwable) -> {
          if (throwable != null) {
            promise.fail(throwable);
          } else {
            promise.complete();
          }
        });
    } else {
      promise.complete();
    }
    return promise.future();
  }

}
