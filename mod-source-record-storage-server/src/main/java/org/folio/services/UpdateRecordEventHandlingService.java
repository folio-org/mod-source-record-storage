package org.folio.services;

import static org.folio.services.util.EventHandlingUtil.sendEventWithPayload;

import java.util.HashMap;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Component
public class UpdateRecordEventHandlingService implements EventHandlingService {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateRecordEventHandlingService.class);

  private static final String QM_SRS_MARC_BIB_RECORD_UPDATED_EVENT_TYPE = "QM_SRS_MARC_BIB_RECORD_UPDATED";

  private final RecordService recordService;

  @Autowired
  public UpdateRecordEventHandlingService(final RecordService recordService) {
    this.recordService = recordService;
  }

  /**
   * Handles QM_RECORD_UPDATED event and sends QM_SRS_MARC_BIB_RECORD_UPDATED as a result of the update
   *
   * {@inheritDoc}
   */
  @Override
  public Future<Boolean> handleEvent(String eventContent, OkapiConnectionParams params) {
    try {
      HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(eventContent), HashMap.class);
      String parsedRecordDtoAsString = eventPayload.get("PARSED_RECORD_DTO");
      String snapshotId = eventPayload.getOrDefault("SNAPSHOT_ID", UUID.randomUUID().toString());

      if (StringUtils.isEmpty(parsedRecordDtoAsString)) {
        String error = "Event payload does not contain required PARSED_RECORD_DTO data";
        LOG.error(error);
        return Future.failedFuture(error);
      }
      ParsedRecordDto record = ObjectMapperTool.getMapper().readValue(parsedRecordDtoAsString, ParsedRecordDto.class);

      return recordService.updateSourceRecord(record, snapshotId, params.getTenantId())
        .compose(updatedRecord -> {
          eventPayload.put(Record.RecordType.MARC.value(), Json.encode(updatedRecord));
          return sendEventWithPayload(Json.encode(eventPayload), QM_SRS_MARC_BIB_RECORD_UPDATED_EVENT_TYPE, params);
        });
    } catch (Exception e) {
      LOG.error("Failed to handle QM_RECORD_UPDATED event", e);
      return Future.failedFuture(e);
    }
  }
}
