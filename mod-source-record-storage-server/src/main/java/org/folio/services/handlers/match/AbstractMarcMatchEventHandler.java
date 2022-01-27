package org.folio.services.handlers.match;

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
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.util.TypeConnection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
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
      LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
      future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      return future;
    }
    payload.getEventsChain().add(payload.getEventType());

    String record = context.get(typeConnection.getMarcType().value());
    MatchDetail matchDetail = retrieveMatchDetail(payload);
    if (isValidMatchDetail(matchDetail)) {
      MatchField matchField = prepareMatchField(record, matchDetail);
      recordDao.getMatchedRecords(matchField, typeConnection, 0, 2, payload.getTenant())
        .onSuccess(recordCollection -> processSucceededResult(recordCollection, payload, future))
        .onFailure(throwable -> future.completeExceptionally(new MatchingException(throwable)));
    } else {
      constructError(payload, format(MATCH_DETAIL_IS_NOT_VALID, matchDetail.toString()));
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
    return new MatchField(field, ind1, ind2, subfield, value);
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
  private void processSucceededResult(RecordCollection collection, DataImportEventPayload payload, CompletableFuture<DataImportEventPayload> future) {
    if (collection.getTotalRecords() == 1) {
      payload.setEventType(matchedEventType.toString());
      payload.getContext().put(getMatchedMarcKey(), Json.encode(collection.getRecords().get(0)));
      future.complete(payload);
    } else if (collection.getTotalRecords() > 1) {
      constructError(payload, FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE);
      future.completeExceptionally(new MatchingException(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE));
    } else if (collection.getTotalRecords() == 0) {
      constructError(payload, CANNOT_FIND_RECORDS_ERROR_MESSAGE);
      future.complete(payload);
    }
  }

  /* Logic for processing errors */
  private void constructError(DataImportEventPayload payload, String errorMessage) {
    LOG.error(errorMessage);
    payload.setEventType(notMatchedEventType.toString());
  }
}
