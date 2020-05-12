package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
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

import static org.folio.services.util.EventHandlingUtil.sendEventWithPayload;

@Component
public class UpdateRecordEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateRecordEventHandler.class);

  private static final String QM_SRS_MARC_BIB_RECORD_UPDATED_EVENT_TYPE = "QM_SRS_MARC_BIB_RECORD_UPDATED";

  @Autowired
  private RecordService recordService;

  /**
   * Handles QM_RECORD_UPDATED event and sends QM_SRS_MARC_BIB_RECORD_UPDATED as a result of the update
   *
   * @param eventContent event content
   * @param params       OkapiConnectionParams
   * @return future with true if succeeded
   */
  public Future<Boolean> handle(String eventContent, OkapiConnectionParams params) {
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
    } catch (IOException e) {
      LOG.error("Failed to handle QM_RECORD_UPDATED event", e);
      return Future.failedFuture(e);
    }
  }
}
