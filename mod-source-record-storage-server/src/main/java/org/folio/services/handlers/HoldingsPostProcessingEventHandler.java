package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import org.folio.services.RecordService;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.springframework.stereotype.Component;

import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.TypeConnection;

@Component
public class HoldingsPostProcessingEventHandler extends AbstractPostProcessingEventHandler {

  public HoldingsPostProcessingEventHandler(RecordService recordService, KafkaConfig kafkaConfig,
                                            MappingParametersSnapshotCache mappingParametersCache, Vertx vertx) {
    super(recordService, kafkaConfig, mappingParametersCache, vertx);
  }

  @Override
  protected void sendAdditionalEvent(DataImportEventPayload dataImportEventPayload, Record record) {

  }

  @Override
  protected String getNextEventType(DataImportEventPayload dataImportEventPayload) {
    return dataImportEventPayload.getEventType();
  }

  @Override
  protected DataImportEventTypes replyEventType() {
    return DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;
  }

  @Override
  protected TypeConnection typeConnection() {
    return TypeConnection.MARC_HOLDINGS;
  }

  @Override
  protected void prepareEventPayload(DataImportEventPayload dataImportEventPayload) {

  }

  @Override
  protected void setExternalIds(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid) {
    externalIdsHolder.setHoldingsId(externalId);
    externalIdsHolder.setHoldingsHrid(externalHrid);
  }

  @Override
  protected String getExternalId(Record record) {
    return record.getExternalIdsHolder().getHoldingsId();
  }

  @Override
  protected String getExternalHrid(Record record) {
    return record.getExternalIdsHolder().getHoldingsHrid();
  }

  @Override
  protected boolean isHridFillingNeeded() {
    return true;
  }

  @Override
  protected String extractHrid(Record record, JsonObject externalEntity) {
    return externalEntity.getString(HRID_FIELD);
  }

}
