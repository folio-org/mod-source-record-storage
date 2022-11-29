package org.folio.services.handlers.match;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
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
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.util.TypeConnection;
import org.jooq.Condition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

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

  private final TypeConnection typeConnection;
  private final RecordDao recordDao;
  private final DataImportEventTypes matchedEventType;
  private final DataImportEventTypes notMatchedEventType;

  public AbstractMarcMatchEventHandler(TypeConnection typeConnection, RecordDao recordDao, DataImportEventTypes matchedEventType, DataImportEventTypes notMatchedEventType) {
    this.typeConnection = typeConnection;
    this.recordDao = recordDao;
    this.matchedEventType = matchedEventType;
    this.notMatchedEventType = notMatchedEventType;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    HashMap<String, String> context = payload.getContext();

    if (context == null || context.isEmpty() || isEmpty(payload.getContext().get(typeConnection.getMarcType().value())) || Objects.isNull(payload.getCurrentNode()) || Objects.isNull(payload.getEventsChain())) {
      LOG.warn(PAYLOAD_HAS_NO_DATA_MSG);
      future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      return future;
    }
    LOG.info("TEMP DEBUG: --------------- payload: {}", payload);
    payload.getEventsChain().add(payload.getEventType());
    payload.setAdditionalProperty(USER_ID_HEADER, context.get(USER_ID_HEADER));

    String record = context.get(typeConnection.getMarcType().value());
    MatchDetail matchDetail = retrieveMatchDetail(payload);
    LOG.info("TEMP DEBUG: --------------- record: {}", record);
    LOG.info("TEMP DEBUG: --------------- matchDetail: {}", matchDetail);
    if (isValidMatchDetail(matchDetail)) {
      MatchField matchField = prepareMatchField(record, matchDetail);
      if (matchField.isDefaultField()) {
        processDefaultMatchField(matchField, payload.getTenant())
          .onSuccess(recordCollection -> processSucceededResult(recordCollection.getRecords(), payload, future))
          .onFailure(throwable -> future.completeExceptionally(new MatchingException(throwable)));
      } else {
        recordDao.getMatchedRecords(matchField, typeConnection, 0, 2, payload.getTenant())
          .onSuccess(recordList -> processSucceededResult(recordList, payload, future))
          .onFailure(throwable -> future.completeExceptionally(new MatchingException(throwable)));
      }
    } else {
      constructError(payload, format(MATCH_DETAIL_IS_NOT_VALID, matchDetail));
      future.complete(payload);
    }
    return future;
  }

  /* Creates a {@link MatchField} from the given {@link MatchDetail} */
  private MatchField prepareMatchField(String record, MatchDetail matchDetail) {
    List<Field> matchDetailFields = matchDetail.getExistingMatchExpression().getFields();
    String field = matchDetailFields.get(0).getValue();
    String ind1 = matchDetailFields.get(1).getValue();
    String ind2 = matchDetailFields.get(2).getValue();
    String subfield = matchDetailFields.get(3).getValue();
    String value = retrieveValueFromMarcRecord(record, matchDetail.getIncomingMatchExpression());
    LOG.info("TEMP DEBUG: prepareMatchField: {} {} {} {} {}", field, ind1, ind2, subfield, value);
    return new MatchField(field, ind1, ind2, subfield, value);
  }

  /* Searches for {@link MatchField} in a separate record properties considering it is matched_id, external_id, or external_hrid */
  private Future<RecordCollection> processDefaultMatchField(MatchField matchField, String tenantId) {
    Condition condition = filterRecordByState(Record.State.ACTUAL.value());
    if (matchField.isMatchedId()) {
      condition = condition.and(filterRecordByRecordId(matchField.getValue()));
    } else if (matchField.isExternalId()) {
      condition = condition.and(filterRecordByExternalId(matchField.getValue()));
    } else if (matchField.isExternalHrid()) {
      condition = condition.and(filterRecordByExternalHrid(matchField.getValue()));
    }
    return recordDao.getRecords(condition, typeConnection.getDbType(), new ArrayList<>(), 0, 2, tenantId);
  }

  /* Verifies a correctness of the given {@link MatchDetail} */
  private boolean isValidMatchDetail(MatchDetail matchDetail) {
    if (matchDetail.getExistingMatchExpression() != null && matchDetail.getExistingMatchExpression().getDataValueType() == VALUE_FROM_RECORD) {
      List<Field> fields = matchDetail.getExistingMatchExpression().getFields();
      LOG.info("TEMP DEBUG: isValidMatchDetail: fields {}, size: {}, getIncomingRecordType: {}, getMarcType: {}, getExistingRecordType: {}",
        fields, fields.size(), matchDetail.getIncomingRecordType(), typeConnection.getMarcType(), matchDetail.getExistingRecordType());
      return fields != null && fields.size() == 4 && matchDetail.getIncomingRecordType() == typeConnection.getMarcType() && matchDetail.getExistingRecordType() == typeConnection.getMarcType();
    }
    LOG.info("TEMP DEBUG: isValidMatchDetail: FALSE");
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

  /* Read value from the Marc record using di-core library */
  private String retrieveValueFromMarcRecord(String record, MatchExpression matchExpression) {
    String valueFromField = StringUtils.EMPTY;
    var value = MarcValueReaderUtil.readValueFromRecord(record, matchExpression);
    if (value.getType() == Value.ValueType.STRING) {
      valueFromField = String.valueOf(value.getValue());
    }
    return valueFromField;
  }

  /**
   * Prepares {@link DataImportEventPayload} for the further processing based on the number of retrieved records in {@link RecordCollection}
   */
  private void processSucceededResult(List<Record> records, DataImportEventPayload payload, CompletableFuture<DataImportEventPayload> future) {
    if (records.size() == 1) {
      payload.setEventType(matchedEventType.toString());
      payload.getContext().put(getMatchedMarcKey(), Json.encode(records.get(0)));
      future.complete(payload);
    } else if (records.size() > 1) {
      constructError(payload, FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE);
      future.completeExceptionally(new MatchingException(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE));
    } else {
      constructError(payload, CANNOT_FIND_RECORDS_ERROR_MESSAGE);
      future.complete(payload);
    }
  }

  /* Logic for processing errors */
  private void constructError(DataImportEventPayload payload, String errorMessage) {
    LOG.warn(errorMessage);
    payload.setEventType(notMatchedEventType.toString());
  }
}
