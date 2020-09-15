package org.folio.services.handlers;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.services.util.EventHandlingUtil.sendEventWithPayload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.dao.RecordDao;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.reader.util.MarcValueReaderUtil;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.util.OkapiConnectionParams;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Component
public class MarcBibliographicMatchEventHandler implements EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MarcBibliographicMatchEventHandler.class);
  private static final String FAIL_MSG = "Failed to handle instance event {}";
  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle Instance event, cause event payload context does not contain INSTANCE and/or MARC_BIBLIOGRAPHIC data";
  private static final String RECORD_UPDATED_EVENT_TYPE = "DI_SRS_MARC_BIB_INSTANCE_HRID_SET";
  private static final String DATA_IMPORT_IDENTIFIER = "DI";

  private final RecordDao recordDao;
  private final Vertx vertx;

  @Autowired
  public MarcBibliographicMatchEventHandler(final RecordDao recordDao, Vertx vertx) {
    this.recordDao = recordDao;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    String tenantId = dataImportEventPayload.getTenant();
    String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
    String recordAsString = dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    if (StringUtils.isEmpty(instanceAsString) || StringUtils.isEmpty(recordAsString)) {
      LOG.error(EVENT_HAS_NO_DATA_MSG);
      future.completeExceptionally(new EventProcessingException(EVENT_HAS_NO_DATA_MSG));
      return future;
    }

    ProfileSnapshotWrapper matchingProfileWrapper = dataImportEventPayload.getCurrentNode();
    MatchProfile matchProfile;
    if (matchingProfileWrapper.getContent() instanceof Map) {
      matchProfile = new JsonObject((Map) matchingProfileWrapper.getContent()).mapTo(MatchProfile.class);
    } else {
      matchProfile = (MatchProfile) matchingProfileWrapper.getContent();
    }
    MatchDetail matchDetail = matchProfile.getMatchDetails().get(0);
    Value value = MarcValueReaderUtil.readValueFromRecord(recordAsString, matchDetail.getExistingMatchExpression());
    String valueFromField = StringUtils.EMPTY;
    if (value.getType() == Value.ValueType.STRING) {
      valueFromField = String.valueOf(value.getValue());
    }

    MatchExpression matchExpression = matchDetail.getExistingMatchExpression();
    Condition condition = null;
    if (matchExpression != null && matchExpression.getDataValueType() == VALUE_FROM_RECORD) {
      List<Field> fields = matchExpression.getFields();
      if (fields != null && matchDetail.getIncomingRecordType() == EntityType.MARC_BIBLIOGRAPHIC
        && matchDetail.getExistingRecordType() == EntityType.MARC_BIBLIOGRAPHIC) {
        StringBuilder result = new StringBuilder();
        for (Field field : fields) {
          result.append(field.getValue().trim());
        }
        switch (result.toString()) {
          case "999ffs":
            condition = filterRecordByRecordId(valueFromField);
            break;
          case "999ffi":
            condition = filterRecordByInstanceId(valueFromField);
            break;
          case "001":
            condition = filterRecordByInstanceHrid(valueFromField);
            break;
          default:
            condition = null;
        }
      }
    }
    OkapiConnectionParams params = getConnectionParams(dataImportEventPayload);
    HashMap<String, String> context = dataImportEventPayload.getContext();

    recordDao.getRecords(condition, new ArrayList<>(), 0, 999, tenantId)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          if (ar.result().getTotalRecords() == 1) {
            context.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(ar.result().getRecords().get(0)));
            sendEventWithPayload(Json.encode(context), "DI_INVENTORY_MARC_BIBLIOGRAPHIC_MATCHED", params);
            future.complete(dataImportEventPayload);
          } else if (ar.result().getTotalRecords() > 1) {
            String errorMessage = "Found multiple records matching specified conditions";
            LOG.error(errorMessage);
            sendEventWithPayload(Json.encode(context), "DI_INVENTORY_MARC_BIBLIOGRAPHIC_NOT_MATCHED", params);
            future.completeExceptionally(new MatchingException(errorMessage));
          } else if (ar.result().getTotalRecords() == 0) {
            String errorMessage = "Can`t find records matching specified conditions";
            LOG.error(errorMessage);
            sendEventWithPayload(Json.encode(context), "DI_INVENTORY_MARC_BIBLIOGRAPHIC_NOT_MATCHED", params);
            future.completeExceptionally(new MatchingException(errorMessage));
          }
        } else {
          future.completeExceptionally(new MatchingException(ar.cause()));
        }
      });
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MatchProfile matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return matchProfile.getIncomingRecordType() == MARC_BIBLIOGRAPHIC;
    }
    return false;
  }

  private OkapiConnectionParams getConnectionParams(DataImportEventPayload dataImportEventPayload) {
    OkapiConnectionParams params = new OkapiConnectionParams();
    params.setOkapiUrl(dataImportEventPayload.getOkapiUrl());
    params.setTenantId(dataImportEventPayload.getTenant());
    params.setToken(dataImportEventPayload.getToken());
    params.setVertx(vertx);
    return params;
  }
}
