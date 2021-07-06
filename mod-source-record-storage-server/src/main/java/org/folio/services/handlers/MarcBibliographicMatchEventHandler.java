package org.folio.services.handlers;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.dao.RecordDao;
import org.folio.dao.util.RecordType;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.reader.util.MarcValueReaderUtil;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.rest.jooq.Tables.RECORDS_LB;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handler for MARC-MARC matching/not-matching MARC-record by specific fields.
 */
@Component
public class MarcBibliographicMatchEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String MATCHED_ID_MARC_FIELD = "999ffs";
  private static final String INSTANCE_ID_MARC_FIELD = "999ffi";
  private static final String INSTANCE_HRID_MARC_FIELD = "001";
  private static final String FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE = "Found multiple records matching specified conditions";
  private static final String CANNOT_FIND_RECORDS_ERROR_MESSAGE = "Can`t find records matching specified conditions";
  private static final String CANNOT_FIND_RECORDS_FOR_MARC_FIELD_ERROR_MESSAGE = "Can`t find records by this MARC-field path: %s";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";

  private final RecordDao recordDao;
  private final Vertx vertx;

  @Autowired
  public MarcBibliographicMatchEventHandler(final RecordDao recordDao, Vertx vertx) {
    this.recordDao = recordDao;
    this.vertx = vertx;
  }

  /**
   * Handles MARC_BIB_CREATED event.
   */
  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    HashMap<String, String> context = dataImportEventPayload.getContext();
    if (context == null || context.isEmpty() ||
      isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value())) ||
      Objects.isNull(dataImportEventPayload.getCurrentNode()) ||
      Objects.isNull(dataImportEventPayload.getEventsChain())) {
      LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
      future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      return future;
    }

    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    String recordAsString = context.get(MARC_BIBLIOGRAPHIC.value());
    MatchDetail matchDetail = retrieveMatchDetail(dataImportEventPayload);
    String valueFromField = retrieveValueFromMarcRecord(recordAsString, matchDetail.getIncomingMatchExpression());
    MatchExpression matchExpression = matchDetail.getExistingMatchExpression();

    Condition condition = null;
    String marcFieldPath = null;
    if (matchExpression != null && matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
      List<Field> fields = matchExpression.getFields();
      if (fields != null && matchDetail.getIncomingRecordType() == EntityType.MARC_BIBLIOGRAPHIC
        && matchDetail.getExistingRecordType() == EntityType.MARC_BIBLIOGRAPHIC) {
        marcFieldPath = fields.stream().map(field -> field.getValue().trim()).collect(Collectors.joining());
        condition = buildConditionBasedOnMarcField(valueFromField, marcFieldPath);
      }
    }

    if (condition != null) {
      recordDao.getRecords(condition, RecordType.MARC_BIB, new ArrayList<>(), 0, 999, dataImportEventPayload.getTenant())
        .onComplete(ar -> {
          if (ar.succeeded()) {
            processSucceededResult(dataImportEventPayload, future, context, ar);
          } else {
            future.completeExceptionally(new MatchingException(ar.cause()));
          }
        });
    } else {
      constructError(dataImportEventPayload, format(CANNOT_FIND_RECORDS_FOR_MARC_FIELD_ERROR_MESSAGE, marcFieldPath));
      future.complete(dataImportEventPayload);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MatchProfile matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return matchProfile.getIncomingRecordType() == MARC_BIBLIOGRAPHIC && matchProfile.getExistingRecordType() == MARC_BIBLIOGRAPHIC;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value();
  }

  /**
   * Read value from MARC-file using di-core library.
   *
   * @param recordAsString  - record
   * @param matchExpression - matchExpression
   * @return result - string value from MARC-file from specific field.
   */
  private String retrieveValueFromMarcRecord(String recordAsString, MatchExpression matchExpression) {
    String valueFromField = StringUtils.EMPTY;
    Value value = MarcValueReaderUtil.readValueFromRecord(recordAsString, matchExpression);
    if (value.getType() == Value.ValueType.STRING) {
      valueFromField = String.valueOf(value.getValue());
    }
    return valueFromField;
  }

  /**
   * Logic for retrieving MatchDetail from eventPayload.
   *
   * @param dataImportEventPayload - payload
   * @return - resulted MatchDetail
   */
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
   * Builds Condition for filtering by specific field.
   *
   * @param valueFromField - value by which will be filtered from DB.
   * @param fieldPath      - resulted fieldPath
   * @return - built Condition
   */
  private Condition buildConditionBasedOnMarcField(String valueFromField, String fieldPath) {
    Condition condition;
    switch (fieldPath) {
      case MATCHED_ID_MARC_FIELD:
        condition = filterRecordByRecordId(valueFromField).and(filterRecordByState(Record.State.ACTUAL.value()));
        break;
      case INSTANCE_ID_MARC_FIELD:
        condition = filterRecordByInstanceId(valueFromField).and(filterRecordByState(Record.State.ACTUAL.value()));
        break;
      case INSTANCE_HRID_MARC_FIELD:
        condition = filterRecordByInstanceHrid(valueFromField).and(filterRecordByState(Record.State.ACTUAL.value()));
        break;
      default:
        condition = null;
    }
    return condition;
  }

  /**
   * Process result if it was succeeded.
   */
  private void processSucceededResult(DataImportEventPayload dataImportEventPayload, CompletableFuture<DataImportEventPayload> future, HashMap<String, String> context, io.vertx.core.AsyncResult<org.folio.rest.jaxrs.model.RecordCollection> ar) {
    if (ar.result().getTotalRecords() == 1) {
      dataImportEventPayload.setEventType(DI_SRS_MARC_BIB_RECORD_MATCHED.toString());
      context.put(MATCHED_MARC_BIB_KEY, Json.encode(ar.result().getRecords().get(0)));
      future.complete(dataImportEventPayload);
    } else if (ar.result().getTotalRecords() > 1) {
      constructError(dataImportEventPayload, FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE);
      future.completeExceptionally(new MatchingException(FOUND_MULTIPLE_RECORDS_ERROR_MESSAGE));
    } else if (ar.result().getTotalRecords() == 0) {
      constructError(dataImportEventPayload, CANNOT_FIND_RECORDS_ERROR_MESSAGE);
      future.complete(dataImportEventPayload);
    }
  }

  /**
   * Logic for processing errors.
   */
  private void constructError(DataImportEventPayload dataImportEventPayload, String errorMessage) {
    LOG.error(errorMessage);
    dataImportEventPayload.setEventType(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.toString());
  }
}
