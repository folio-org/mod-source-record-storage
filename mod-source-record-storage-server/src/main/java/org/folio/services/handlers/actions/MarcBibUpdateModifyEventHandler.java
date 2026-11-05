package org.folio.services.handlers.actions;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.services.handlers.match.AbstractMarcMatchEventHandler.CENTRAL_TENANT_ID;
import static org.folio.services.util.AdditionalFieldsUtil.isSubfieldExist;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingProfile;
import org.folio.client.InstanceLinkClient;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.writer.marc.MarcBibRecordModifier;
import org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.services.caches.LinkingRulesCache;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.util.AuthorityLinksUtils;
import org.marc4j.MarcJsonReader;
import org.marc4j.marc.DataField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MarcBibUpdateModifyEventHandler extends AbstractUpdateModifyEventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String UNEXPECTED_PAYLOAD_MSG = "Matched record doesn't contains external record id. jobExecutionId '%s'";
  private static final char AUTHORITY_ID_SUBFIELD = '9';
  private static final char AUTHORITY_NATURAL_ID_SUBFIELD = '0';

  private final InstanceLinkClient instanceLinkClient;
  private final LinkingRulesCache linkingRulesCache;

  @Autowired
  public MarcBibUpdateModifyEventHandler(RecordService recordService,
                                         SnapshotService snapshotService,
                                         MappingParametersSnapshotCache mappingParametersCache,
                                         InstanceLinkClient instanceLinkClient,
                                         LinkingRulesCache linkingRulesCache) {
    super(recordService, snapshotService, mappingParametersCache);
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
  protected String getUpdateEventType() {
    return DI_SRS_MARC_BIB_RECORD_UPDATED.value();
  }

  @Override
  protected EntityType modifiedEntityType() {
    return MARC_BIBLIOGRAPHIC;
  }

  @Override
  protected EntityType getRelatedEntityType() {
    return EntityType.INSTANCE;
  }

  @Override
  protected boolean isCentralTenantRecordUpdateProtected() {
    return true;
  }

  @Override
  protected boolean is003FieldRemovalNeeded() {
    return true;
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
    var centralTenantId = dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID);
    if (centralTenantId != null) {
      okapiParams = overrideOkapiParamsWithTenant(okapiParams, centralTenantId);
    }

    ConnectionParams finalOkapiParams = okapiParams;
    if (isAuthorityControlUpdate(dataImportEventPayload)) {
      return linkingRulesCache.get(finalOkapiParams)
        .compose(linkingRuleDtos -> loadInstanceLink(matchedRecord, instanceId, finalOkapiParams)
          .compose(links -> modifyMarcBibRecordWithIncomingLinks(dataImportEventPayload, mappingProfile,
            mappingParameters, instanceId, links, linkingRuleDtos.orElse(Collections.emptyList())))
          .compose(links -> updateInstanceLinks(instanceId, links, finalOkapiParams)));
    }
    return linkingRulesCache.get(finalOkapiParams)
      .compose(linkingRuleDtos -> loadInstanceLink(matchedRecord, instanceId, finalOkapiParams)
        .compose(links -> modifyMarcBibRecord(dataImportEventPayload, mappingProfile, mappingParameters, links, linkingRuleDtos.orElse(Collections.emptyList())))
        .compose(links -> updateInstanceLinks(instanceId, links, finalOkapiParams)));
  }

  private ConnectionParams overrideOkapiParamsWithTenant(ConnectionParams okapiParams, String tenantId) {
    Map<String, String> newHeaders = new HashMap<>(okapiParams.getHeaders());
    newHeaders.put(XOkapiHeaders.TENANT, tenantId);
    return new ConnectionParams(newHeaders);
  }

  private Future<Optional<InstanceLinkDtoCollection>> loadInstanceLink(Record oldRecord, String instanceId,
                                                                       ConnectionParams okapiParams) {
    Promise<Optional<InstanceLinkDtoCollection>> promise = Promise.promise();
    if (isSubfieldExist(oldRecord, AuthorityLinksUtils.AUTHORITY_ID_SUBFIELD)) {
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

  /**
   * POC (MODBULKOPS-690): batch authority control from bulk edit is recognized by its hidden
   * MARC bib update action profile that keeps $9 (remove9Subfields = false).
   */
  private boolean isAuthorityControlUpdate(DataImportEventPayload payload) {
    return findParentActionProfile(payload.getProfileSnapshot(), payload.getCurrentNode().getId())
      .map(actionProfile -> TRUE.equals(actionProfile.getHidden())
        && FALSE.equals(actionProfile.getRemove9Subfields()))
      .orElse(false);
  }

  private Optional<ActionProfile> findParentActionProfile(ProfileSnapshotWrapper node, String childId) {
    if (node == null) {
      return Optional.empty();
    }
    for (ProfileSnapshotWrapper child : node.getChildSnapshotWrappers()) {
      if (node.getContentType() == ACTION_PROFILE && child.getId().equals(childId)) {
        return Optional.of(JsonObject.mapFrom(node.getContent()).mapTo(ActionProfile.class));
      }
      var actionProfile = findParentActionProfile(child, childId);
      if (actionProfile.isPresent()) {
        return actionProfile;
      }
    }
    return Optional.empty();
  }

  /**
   * Incoming fields replace the stored ones together with their $9,
   * then the instance links are rebuilt from $9 of the updated record.
   */
  private Future<Optional<InstanceLinkDtoCollection>> modifyMarcBibRecordWithIncomingLinks(
    DataImportEventPayload dataImportEventPayload, MappingProfile mappingProfile, MappingParameters mappingParameters,
    String instanceId, Optional<InstanceLinkDtoCollection> links, List<LinkingRuleDto> linkingRules) {
    try {
      MarcRecordModifier marcRecordModifier = new MarcRecordModifier();
      marcRecordModifier.initialize(dataImportEventPayload, mappingParameters, mappingProfile, modifiedEntityType());
      marcRecordModifier.modifyRecord(mappingProfile.getMappingDetails().getMarcMappingDetails());
      marcRecordModifier.getResult(dataImportEventPayload);

      var existingLinks = links.orElse(new InstanceLinkDtoCollection().withLinks(new ArrayList<>()));
      var updatedRecord = extractRecord(dataImportEventPayload, "MATCHED_" + modifiedEntityType().value());
      var updatedLinks = buildLinks(updatedRecord, instanceId, existingLinks.getLinks(), linkingRules);
      if (isLinksTheSame(existingLinks, updatedLinks)) {
        return Future.succeededFuture(Optional.empty());
      }
      return Future.succeededFuture(Optional.of(new InstanceLinkDtoCollection().withLinks(updatedLinks)));
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private List<Link> buildLinks(Record updatedRecord, String instanceId, List<Link> existingLinks,
                                List<LinkingRuleDto> linkingRules) {
    var rulesByTag = linkingRules.stream().collect(Collectors.groupingBy(LinkingRuleDto::getBibField));
    var tagByRuleId = linkingRules.stream()
      .collect(Collectors.toMap(LinkingRuleDto::getId, LinkingRuleDto::getBibField));
    var links = new ArrayList<Link>();
    for (DataField dataField : toMarcRecord(updatedRecord).getDataFields()) {
      var tag = dataField.getTag();
      var authorityId = dataField.getSubfield(AUTHORITY_ID_SUBFIELD);
      var tagRules = rulesByTag.getOrDefault(tag, Collections.emptyList());
      if (authorityId == null || tagRules.isEmpty()) {
        continue;
      }
      var existingLink = existingLinks.stream()
        .filter(link -> authorityId.getData().equals(link.getAuthorityId())
          && tag.equals(tagByRuleId.get(link.getLinkingRuleId())))
        .findFirst();
      var naturalId = dataField.getSubfield(AUTHORITY_NATURAL_ID_SUBFIELD);
      if (existingLink.isPresent()) {
        if (!links.contains(existingLink.get())) {
          links.add(existingLink.get());
        }
      } else if (tagRules.size() == 1 && naturalId != null) {
        var naturalIdValue = naturalId.getData();
        links.add(new Link()
          .withInstanceId(instanceId)
          .withAuthorityId(authorityId.getData())
          .withAuthorityNaturalId(naturalIdValue.substring(naturalIdValue.lastIndexOf('/') + 1))
          .withLinkingRuleId(tagRules.getFirst().getId()));
      } else {
        LOG.warn("buildLinks:: link for field '{}' with authority '{}' is skipped: linking rule cannot be resolved",
          tag, authorityId.getData());
      }
    }
    return links;
  }

  private org.marc4j.marc.Record toMarcRecord(Record marcRecord) {
    var content = marcRecord.getParsedRecord().getContent();
    var json = content instanceof String s ? s : Json.encode(content);
    return new MarcJsonReader(new ByteArrayInputStream(json.getBytes(UTF_8))).next();
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
                                           ConnectionParams okapiParams) {
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
