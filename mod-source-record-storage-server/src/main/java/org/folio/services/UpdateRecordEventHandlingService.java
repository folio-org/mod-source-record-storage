package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;

import org.folio.rest.util.OkapiConnectionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.UUID;

import static org.folio.services.util.EventHandlingUtil.sendEventWithPayloadToPubSub;

@Component
public class UpdateRecordEventHandlingService implements EventHandlingService {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateRecordEventHandlingService.class);

  private static final String QM_SRS_MARC_BIB_RECORD_UPDATED_EVENT_TYPE = "QM_SRS_MARC_BIB_RECORD_UPDATED";
  private static final String QM_ERROR_EVENT_TYPE = "QM_ERROR";

  private final RecordService recordService;

  @Autowired
  public UpdateRecordEventHandlingService(final RecordService recordService) {
    this.recordService = recordService;
  }

  /**
   * Handles QM_RECORD_UPDATED event and sends QM_SRS_MARC_BIB_RECORD_UPDATED as a result of the update
   * <p>
   * {@inheritDoc}
   */
  @Override
  public Future<Boolean> handleEvent(String eventContent, OkapiConnectionParams params) {
    try {
      HashMap<String, String> eventPayload = new ObjectMapper().readValue(ZIPArchiver.unzip(eventContent), HashMap.class);
      String parsedRecordDtoAsString = eventPayload.get("PARSED_RECORD_DTO");
      String snapshotId = eventPayload.getOrDefault("SNAPSHOT_ID", UUID.randomUUID().toString());

      if (StringUtils.isEmpty(parsedRecordDtoAsString)) {
        String error = "Event payload does not contain required PARSED_RECORD_DTO data";
        LOG.error(error);
        return Future.failedFuture(error);
      }
      ParsedRecordDto record = new ObjectMapper().readValue(parsedRecordDtoAsString, ParsedRecordDto.class);

      return recordService.updateSourceRecord(record, snapshotId, params.getTenantId())
        .compose(updatedRecord -> {
          eventPayload.put(Record.RecordType.MARC.value(), Json.encode(updatedRecord));
          return sendEventWithPayloadToPubSub(Json.encode(eventPayload), QM_SRS_MARC_BIB_RECORD_UPDATED_EVENT_TYPE, params);
        })
        .onFailure(f -> sendEventWithPayloadToPubSub(Json.encode(eventPayload), QM_ERROR_EVENT_TYPE, params));
    } catch (Exception e) {
      LOG.error("Failed to handle QM_RECORD_UPDATED event", e);
      return Future.failedFuture(e);
    }
  }
}
