package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import java.util.List;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.springframework.stereotype.Component;

import org.folio.DataImportEventPayload;
import org.folio.dao.RecordDao;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.util.AdditionalFieldsUtil;
import org.folio.services.util.TypeConnection;

@Component
public class AuthorityPostProcessingEventHandler extends AbstractPostProcessingEventHandler {

  private final KafkaConfig kafkaConfig;

  public AuthorityPostProcessingEventHandler(RecordDao recordDao, KafkaConfig kafkaConfig,
                                             MappingParametersSnapshotCache mappingParamsCache,
                                             Vertx vertx) {
    super(recordDao, kafkaConfig, mappingParamsCache, vertx);
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  protected void sendAdditionalEvent(DataImportEventPayload dataImportEventPayload, Record record) {
    if (isLogNeeded(record, dataImportEventPayload.getEventType())) {
      var key = getEventKey();
      var kafkaHeaders = getKafkaHeaders(dataImportEventPayload);
      if (record.getGeneration() > 0) {
        sendLogEvent(dataImportEventPayload, DI_LOG_SRS_MARC_AUTHORITY_RECORD_UPDATED, key, kafkaHeaders);
      } else if (record.getGeneration() == 0) {
        sendLogEvent(dataImportEventPayload, DI_LOG_SRS_MARC_AUTHORITY_RECORD_CREATED, key, kafkaHeaders);
      }
    }
  }

  @Override
  protected DataImportEventTypes replyEventType() {
    return null;
  }

  @Override
  protected TypeConnection typeConnection() {
    return TypeConnection.MARC_AUTHORITY;
  }

  @Override
  protected void setExternalIds(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid) {
    externalIdsHolder.setAuthorityId(externalId);
    externalIdsHolder.setAuthorityHrid(externalHrid);
  }

  @Override
  protected String getExternalId(Record record) {
    return record.getExternalIdsHolder().getAuthorityId();
  }

  @Override
  protected String getExternalHrid(Record record) {
    return record.getExternalIdsHolder().getAuthorityHrid();
  }

  @Override
  protected boolean isHridFillingNeeded() {
    return false;
  }

  @Override
  protected String extractHrid(Record record, JsonObject externalEntity) {
    return AdditionalFieldsUtil.getValueFromControlledField(record, HR_ID_FROM_FIELD);
  }

  private void sendLogEvent(DataImportEventPayload payload, DataImportEventTypes logEventType, String key,
                            List<KafkaHeader> kafkaHeaders) {
    payload.setEventType(logEventType.value());
    sendEventToKafka(payload.getTenant(), Json.encode(payload), logEventType.value(), kafkaHeaders, kafkaConfig, key);
  }

  private boolean isLogNeeded(Record record, String eventType) {
    return record.getGeneration() != null
      && (DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value().equals(eventType)
      || DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value().equals(eventType));
  }
}
