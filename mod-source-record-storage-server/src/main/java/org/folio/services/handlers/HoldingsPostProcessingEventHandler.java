package org.folio.services.handlers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_HOLDING_HRID_SET;

import org.springframework.stereotype.Component;

import org.folio.DataImportEventPayload;
import org.folio.dao.RecordDao;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.TypeConnection;

@Component
public class HoldingsPostProcessingEventHandler extends AbstractPostProcessingEventHandler {

  public HoldingsPostProcessingEventHandler(RecordDao recordDao, KafkaConfig kafkaConfig) {
    super(recordDao, kafkaConfig);
  }

  @Override
  protected void sendAdditionalEvent(DataImportEventPayload dataImportEventPayload, Record record) {

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
  protected void setExternalIds(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid) {
    externalIdsHolder.setHoldingsId(externalId);
    externalIdsHolder.setHoldingsHrid(externalHrid);
  }

}
