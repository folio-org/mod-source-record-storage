package org.folio.services.handlers.match;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.dao.RecordDao;
import org.folio.dao.util.MatchField;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.reader.util.MarcValueReaderUtil;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.Filter;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Qualifier;
import org.folio.rest.jaxrs.model.ReactToType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.util.RestUtil;
import org.folio.services.util.TypeConnection;
import org.jooq.Condition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByMultipleIds;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;

/**
 * Abstract handler for MARC-MARC matching/not-matching of Marc record by specific fields
 */
public abstract class AbstractMarcMatchEventHandler implements EventHandler {
  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE = "Found multiple records matching specified conditions";
  private static final String CANNOT_FIND_RECORDS_ERROR_MESSAGE = "Can`t find records matching specified conditions";
  private static final String MATCH_DETAIL_IS_NOT_VALID = "Match detail is not valid: %s";
  private static final String USER_ID_HEADER = "userId";
  public static final String CENTRAL_TENANT_ID = "CENTRAL_TENANT_ID";
  public static final String MULTI_MATCH_IDS = "MULTI_MATCH_IDS";

  private final TypeConnection typeConnection;
  private final RecordDao recordDao;
  private final DataImportEventTypes matchedEventType;
  private final DataImportEventTypes notMatchedEventType;
  private final ConsortiumConfigurationCache consortiumConfigurationCache;
  private final Vertx vertx;

  protected AbstractMarcMatchEventHandler(TypeConnection typeConnection, RecordDao recordDao, DataImportEventTypes matchedEventType,
                                          DataImportEventTypes notMatchedEventType, ConsortiumConfigurationCache consortiumConfigurationCache,
                                          Vertx vertx) {
    this.typeConnection = typeConnection;
    this.recordDao = recordDao;
    this.matchedEventType = matchedEventType;
    this.notMatchedEventType = notMatchedEventType;
    this.consortiumConfigurationCache = consortiumConfigurationCache;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    try {
      HashMap<String, String> context = payload.getContext();

      if (MapUtils.isEmpty(context) || isEmpty(payload.getContext().get(typeConnection.getMarcType().value())) || Objects.isNull(payload.getCurrentNode()) || Objects.isNull(payload.getEventsChain())) {
        LOG.warn(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      payload.getEventsChain().add(payload.getEventType());
      payload.setAdditionalProperty(USER_ID_HEADER, context.get(USER_ID_HEADER));

      String record = context.get(typeConnection.getMarcType().value());
      MatchDetail matchDetail = retrieveMatchDetail(payload);
      if (isValidMatchDetail(matchDetail)) {
        MatchField matchField = prepareMatchField(record, matchDetail);
        Filter.ComparisonPartType comparisonPartType = prepareComparisonPartType(matchDetail);
        return retrieveMarcRecords(matchField, comparisonPartType, payload, payload.getTenant())
          .compose(localMatchedRecords -> {
            if (isConsortiumAvailable()) {
              return matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords(payload, matchField, comparisonPartType, localMatchedRecords);
            }
            return Future.succeededFuture(localMatchedRecords);
          })
          .compose(recordList -> processSucceededResult(recordList, payload))
          .recover(throwable -> Future.failedFuture(mapToMatchException(throwable)))
          .toCompletionStage().toCompletableFuture();
      }
      constructError(payload, format(MATCH_DETAIL_IS_NOT_VALID, matchDetail));
      return CompletableFuture.completedFuture(payload);
    } catch (Exception e) {
      LOG.warn("handle:: Error while processing event for MARC record matching", e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private Future<List<Record>> matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords(DataImportEventPayload payload,
                                                                                           MatchField matchField,
                                                                                           Filter.ComparisonPartType comparisonPartType,
                                                                                           List<Record> recordList) {
    return consortiumConfigurationCache.get(RestUtil.retrieveOkapiConnectionParams(payload, vertx))
      .compose(consortiumConfigurationOptional -> {
        if (consortiumConfigurationOptional.isPresent() && !consortiumConfigurationOptional.get().getCentralTenantId().equals(payload.getTenant())) {
          LOG.debug("matchCentralTenantIfNeededAndCombineWithLocalMatchedRecords:: Matching on centralTenant with id: {}",
            consortiumConfigurationOptional.get().getCentralTenantId());
          return retrieveMarcRecords(matchField, comparisonPartType, payload, consortiumConfigurationOptional.get().getCentralTenantId())
            .onSuccess(result -> {
              if (!result.isEmpty()) {
                payload.getContext().put(CENTRAL_TENANT_ID, consortiumConfigurationOptional.get().getCentralTenantId());
              }
            })
            .map(centralTenantResult -> Stream.concat(recordList.stream(), centralTenantResult.stream()).toList());
        }
        return Future.succeededFuture(recordList);
      });
  }

  private static Throwable mapToMatchException(Throwable throwable) {
    return throwable instanceof MatchingException ? throwable : new MatchingException(throwable);
  }

  private Future<List<Record>> retrieveMarcRecords(MatchField matchField, Filter.ComparisonPartType comparisonPartType,
                                                   DataImportEventPayload payload, String tenant) {
    List<String> matchedRecordIds = getMatchedRecordIds(payload);
    if (matchField.isDefaultField()) {
      LOG.debug("retrieveMarcRecords:: Process default field matching, matchField {}, tenant {}", matchField, tenant);
      return processDefaultMatchField(matchField, matchedRecordIds, tenant).map(RecordCollection::getRecords);
    }

    LOG.debug("retrieveMarcRecords:: Process matched field matching, matchField {}, tenant {}", matchField, tenant);
    return recordDao.getMatchedRecords(matchField, comparisonPartType, matchedRecordIds, typeConnection, isNonNullExternalIdRequired(), 0, 2, tenant);
  }

  abstract boolean isConsortiumAvailable();

  /* Creates a {@link MatchField} from the given {@link MatchDetail} */
  private MatchField prepareMatchField(String record, MatchDetail matchDetail) {
    List<Field> matchDetailFields = matchDetail.getExistingMatchExpression().getFields();
    String field = matchDetailFields.get(0).getValue();
    String ind1 = matchDetailFields.get(1).getValue();
    String ind2 = matchDetailFields.get(2).getValue();
    String subfield = matchDetailFields.get(3).getValue();
    Value value = MarcValueReaderUtil.readValueFromRecord(record, matchDetail.getIncomingMatchExpression());
    return new MatchField(field, ind1, ind2, subfield, value);
  }

  private Filter.ComparisonPartType prepareComparisonPartType(MatchDetail matchDetail) {
    Qualifier qualifier = matchDetail.getExistingMatchExpression().getQualifier();
    return (qualifier != null) ? Filter.ComparisonPartType.valueOf(qualifier.getComparisonPart().toString()) : null;
  }

  /* Searches for {@link MatchField} in a separate record properties considering it is matched_id, external_id, or external_hrid */
  private Future<RecordCollection> processDefaultMatchField(MatchField matchField, List<String> multipleRecordIds, String tenantId) {
    Condition condition = filterRecordByMultipleIds(multipleRecordIds);

    condition.and(filterRecordByState(Record.State.ACTUAL.value()));
    String valueAsString = getStringValue(matchField.getValue());
    if (matchField.isMatchedId()) {
      condition = condition.and(filterRecordByRecordId(valueAsString));
    } else if (matchField.isExternalId()) {
      condition = condition.and(filterRecordByExternalId(valueAsString));
    } else if (matchField.isExternalHrid()) {
      condition = condition.and(filterRecordByExternalHrid(valueAsString));
    }
    return recordDao.getRecords(condition, typeConnection.getDbType(), new ArrayList<>(), 0, 2, tenantId);
  }

  private String getStringValue(Value value) {
    if (Value.ValueType.STRING.equals(value.getType())) {
      return String.valueOf(value.getValue());
    }
    return StringUtils.EMPTY;
  }

  /* Verifies a correctness of the given {@link MatchDetail} */
  private boolean isValidMatchDetail(MatchDetail matchDetail) {
    if (matchDetail.getExistingMatchExpression() != null && matchDetail.getExistingMatchExpression().getDataValueType() == VALUE_FROM_RECORD) {
      List<Field> fields = matchDetail.getExistingMatchExpression().getFields();
      return fields != null && fields.size() == 4 && matchDetail.getIncomingRecordType() == typeConnection.getMarcType() && matchDetail.getExistingRecordType() == typeConnection.getMarcType();
    }
    return false;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      var matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return isEligibleMatchProfile(matchProfile);
    }
    return false;
  }

  /**
   * @return the key under which a matched record is put into event payload context
   */
  protected String getMatchedMarcKey() {
    return "MATCHED_" + typeConnection.getMarcType();
  }

  protected boolean isNonNullExternalIdRequired() {
    return false;
  }

  /* Verifies whether the given {@link MatchProfile} is suitable for {@link EventHandler} */
  private boolean isEligibleMatchProfile(MatchProfile matchProfile) {
    return matchProfile.getIncomingRecordType() == typeConnection.getMarcType()
      && matchProfile.getExistingRecordType() == typeConnection.getMarcType();
  }

  /* Retrieves a {@link MatchDetail} from the given {@link DataImportEventPayload} */
  private MatchDetail retrieveMatchDetail(DataImportEventPayload dataImportEventPayload) {
    MatchProfile matchProfile;
    ProfileSnapshotWrapper matchingProfileWrapper = dataImportEventPayload.getCurrentNode();
    if (matchingProfileWrapper.getContent() instanceof Map) {
      matchProfile = new JsonObject((Map) matchingProfileWrapper.getContent()).mapTo(MatchProfile.class);
    } else {
      matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
    }
    return matchProfile.getMatchDetails().get(0);
  }

  /**
   * Prepares {@link DataImportEventPayload} for the further processing based on the number of retrieved records in {@link RecordCollection}
   */
  private Future<DataImportEventPayload> processSucceededResult(List<Record> records, DataImportEventPayload payload) {
    if (records.size() == 1) {
      payload.setEventType(matchedEventType.toString());
      payload.getContext().put(getMatchedMarcKey(), Json.encode(records.get(0)));
      LOG.debug("processSucceededResult:: Matched 1 record for tenant with id {}", payload.getTenant());
      return Future.succeededFuture(payload);
    }
    if (records.size() > 1) {
      if (canProcessMultiMatchResult(payload)) {
        String multipleRecordIdsList = mapEntityListToIdsJsonString(records);
        LOG.debug("processSucceededResult:: Found multiple records, proceed matching. Records: {}", multipleRecordIdsList);
        payload.setEventType(matchedEventType.toString());
        payload.getContext().put(MULTI_MATCH_IDS, multipleRecordIdsList);
        return Future.succeededFuture(payload);
      } else {
        constructError(payload, FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE);
        LOG.warn("processSucceededResult:: Matched multiple record for tenant with id {}", payload.getTenant());
        return Future.failedFuture(new MatchingException(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE));
      }
    }
    constructError(payload, CANNOT_FIND_RECORDS_ERROR_MESSAGE);
    return Future.succeededFuture(payload);
  }

  private boolean canProcessMultiMatchResult(DataImportEventPayload eventPayload) {
    List<ProfileSnapshotWrapper> childProfiles = eventPayload.getCurrentNode().getChildSnapshotWrappers();
    return isNotEmpty(childProfiles) && ReactToType.MATCH.equals(childProfiles.get(0).getReactTo())
      && MATCH_PROFILE.equals(childProfiles.get(0).getContentType());
  }

  /* Logic for processing errors */
  private void constructError(DataImportEventPayload payload, String errorMessage) {
    LOG.warn(errorMessage);
    payload.setEventType(notMatchedEventType.toString());
  }

  private String mapEntityListToIdsJsonString(List<Record> records) {
    List<String> idList = records.stream()
      .map(Record::getId)
      .collect(Collectors.toList());

    return Json.encode(idList);
  }

  private List<String> getMatchedRecordIds(DataImportEventPayload payload) {
    if (StringUtils.isNotEmpty(payload.getContext().get(MULTI_MATCH_IDS))) {
      return new JsonArray(payload.getContext().remove(MULTI_MATCH_IDS)).stream().map(o -> (String) o).toList();
    }
    return Collections.emptyList();
  }
}
