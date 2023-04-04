package org.folio.services.handlers.actions;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.services.util.AdditionalFieldsUtil.isSubfieldExist;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingProfile;
import org.folio.client.InstanceLinkClient;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcBibRecordModifier;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.caches.LinkingRulesCache;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MarcBibUpdateModifyEventHandler extends AbstractUpdateModifyEventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String UNEXPECTED_PAYLOAD_MSG = "Matched record doesn't contains external record id. jobExecutionId '%s'";
  private static final char SUB_FIELD_9 = '9';

  private final InstanceLinkClient instanceLinkClient;
  private final LinkingRulesCache linkingRulesCache;

  @Autowired
  public MarcBibUpdateModifyEventHandler(RecordService recordService,
                                         MappingParametersSnapshotCache mappingParametersCache,
                                         Vertx vertx, InstanceLinkClient instanceLinkClient,
                                         LinkingRulesCache linkingRulesCache) {
    super(recordService, mappingParametersCache, vertx);
    this.instanceLinkClient = instanceLinkClient;
    this.linkingRulesCache = linkingRulesCache;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected boolean isHridFillingNeeded() {
    return false;
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
  protected Future<Void> modifyRecord(DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile,
                                      MappingParameters mappingParameters) {
    if (mappingProfile.getMappingDetails().getMarcMappingOption() == MappingDetail.MarcMappingOption.MODIFY) {
      return modifyMarcBibRecord(dataImportEventPayload, mappingProfile, mappingParameters, Optional.empty(), Collections.emptyList())
        .map(v -> null);
    }
    var matchedRecord = extractRecord(dataImportEventPayload, "MATCHED_" + modifiedEntityType().value());
    var isValid = matchedRecord != null && matchedRecord.getExternalIdsHolder() != null;
    if (!isValid) {
      var msg = String.format(UNEXPECTED_PAYLOAD_MSG, dataImportEventPayload.getJobExecutionId());
      LOG.warn(msg);
      throw new EventProcessingException(msg);
    }
    var instanceId = matchedRecord.getExternalIdsHolder().getInstanceId();
    var okapiParams = getOkapiParams(dataImportEventPayload);

    return linkingRulesCache.get(okapiParams)
      .compose(linkingRuleDtos -> loadInstanceLink(matchedRecord, instanceId, okapiParams)
        .compose(links -> modifyMarcBibRecord(dataImportEventPayload, mappingProfile, mappingParameters, links, linkingRuleDtos.orElse(Collections.emptyList())))
        .compose(links -> updateInstanceLinks(instanceId, links, okapiParams)));
  }

  private Future<Optional<InstanceLinkDtoCollection>> loadInstanceLink(Record oldRecord, String instanceId,
                                                                       OkapiConnectionParams okapiParams) {
    Promise<Optional<InstanceLinkDtoCollection>> promise = Promise.promise();
    if (isSubfieldExist(oldRecord, SUB_FIELD_9)) {
      if (isNull(instanceId) || isBlank(instanceId)) {
        instanceId = oldRecord.getExternalIdsHolder().getInstanceId();
      }
      instanceLinkClient.getLinksByInstanceId(instanceId, okapiParams)
        .whenComplete((instanceLinkDtoCollection, throwable) -> {
          if (throwable != null) {
            LOG.error(throwable.getMessage());
            promise.fail(throwable);
          } else {
            promise.complete(instanceLinkDtoCollection);
          }
        });
    } else {
      promise.complete(Optional.empty());
    }

    return promise.future();
  }

  private Future<Optional<InstanceLinkDtoCollection>> modifyMarcBibRecord(DataImportEventPayload dataImportEventPayload,
                                                                          MappingProfile mappingProfile,
                                                                          MappingParameters mappingParameters,
                                                                          Optional<InstanceLinkDtoCollection> links,
                                                                          List<LinkingRuleDto> linkingRules) {
    Promise<Optional<InstanceLinkDtoCollection>> promise = Promise.promise();
    try {
      if (links.isPresent()) {
        MarcBibRecordModifier marcRecordModifier = new MarcBibRecordModifier();
        marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType(),
          links.get(), linkingRules);
        marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
        marcRecordModifier.getResult(dataImportEventPayload);
        if (isLinksTheSame(links.get(), marcRecordModifier.getBibAuthorityLinksKept())) {
          promise.complete(Optional.empty());
        } else {
          promise.complete(
            Optional.of(new InstanceLinkDtoCollection().withLinks(marcRecordModifier.getBibAuthorityLinksKept())));
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

  private Future<Void> updateInstanceLinks(String instanceId, Optional<InstanceLinkDtoCollection> links,
                                           OkapiConnectionParams okapiParams) {
    Promise<Void> promise = Promise.promise();
    if (links.isPresent()) {
      instanceLinkClient.updateInstanceLinks(instanceId, links.get(), okapiParams)
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
