package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_INSTANCE_HRID_SET;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.folio.services.caches.MappingParametersSnapshotCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.folio.DataImportEventPayload;
import org.folio.dao.RecordDao;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.TypeConnection;

@Component
public class InstancePostProcessingEventHandler extends AbstractPostProcessingEventHandler {

  private final KafkaConfig kafkaConfig;

  @Autowired
  public InstancePostProcessingEventHandler(final RecordDao recordDao, KafkaConfig kafkaConfig,
                                            MappingParametersSnapshotCache mappingParametersCache, Vertx vertx) {
    super(recordDao, kafkaConfig, mappingParametersCache, vertx);
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  protected void sendAdditionalEvent(DataImportEventPayload dataImportEventPayload, Record record) {
    sendEventToDataImportLog(dataImportEventPayload, record);
  }

  protected DataImportEventTypes replyEventType() {
    return DI_SRS_MARC_BIB_INSTANCE_HRID_SET;
  }

  @Override
  protected TypeConnection typeConnection() {
    return TypeConnection.MARC_BIB;
  }

  @Override
  protected void setExternalIds(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid) {
    externalIdsHolder.setInstanceId(externalId);
    externalIdsHolder.setInstanceHrid(externalHrid);
  }

  @Override
  protected String getExternalId(Record record) {
    return record.getExternalIdsHolder().getInstanceId();
  }

  @Override
  protected String getExternalHrid(Record record) {
    return record.getExternalIdsHolder().getInstanceHrid();
  }

  @Override
  protected boolean isHridFillingNeeded() {
    return true;
  }

  @Override
  protected String extractHrid(Record record, JsonObject externalEntity) {
    return externalEntity.getString(HRID_FIELD);
  }

  // MODSOURMAN-384: sent event to log when record updated implicitly only for INSTANCE_UPDATED case
  private void sendEventToDataImportLog(DataImportEventPayload dataImportEventPayload, Record record) {
    var key = getEventKey();
    var kafkaHeaders = getKafkaHeaders(dataImportEventPayload);
    var eventType = dataImportEventPayload.getEventType();
    if (isLogRequired(record, eventType)) {
      if (record.getGeneration() > 0) {
        dataImportEventPayload.setEventType(DI_LOG_SRS_MARC_BIB_RECORD_UPDATED.value());
        sendEventToKafka(dataImportEventPayload.getTenant(), Json.encode(dataImportEventPayload),
          DI_LOG_SRS_MARC_BIB_RECORD_UPDATED.value(),
          kafkaHeaders, kafkaConfig, key);
      } else if (record.getGeneration() == 0) {
        dataImportEventPayload.setEventType(DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value());
        sendEventToKafka(dataImportEventPayload.getTenant(), Json.encode(dataImportEventPayload),
          DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value(),
          kafkaHeaders, kafkaConfig, key);
      }
    }
  }

  private boolean isLogRequired(Record record, String eventType) {
    return record.getGeneration() != null
      && (DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value().equals(eventType)
      || DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value().equals(eventType));
  }
}
