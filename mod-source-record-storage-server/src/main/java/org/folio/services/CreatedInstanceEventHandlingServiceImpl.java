package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.folio.dao.RecordDao;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.services.util.AdditionalFieldsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

@Component()
public class CreatedInstanceEventHandlingServiceImpl implements EventHandlingService {

  private static final Logger LOG = LoggerFactory.getLogger(CreatedInstanceEventHandlingServiceImpl.class);

  @Autowired
  private RecordDao recordDao;

  @Override
  public Future<Boolean> handle(String eventContent, String tenantId) {
    try {
      DataImportEventPayload dataImportEventPayload = ObjectMapperTool.getMapper().readValue(eventContent, DataImportEventPayload.class);
      String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
      String recordAsString = dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());

      if (StringUtils.isEmpty(instanceAsString) || StringUtils.isEmpty(recordAsString)) {
        String msg = "Failed to handle CREATED_INVENTORY_INSTANCE event, cause event payload context does not contain INSTANCE and MARC_BIBLIOGRAPHIC data";
        LOG.error(msg);
        return Future.failedFuture(msg);
      }
      JsonObject instanceJson = new JsonObject(instanceAsString);
      Record record = ObjectMapperTool.getMapper().readValue(recordAsString, Record.class);

      return setInstanceIdToRecord(record, instanceJson.getString("id"), tenantId)
        .map(true);
    } catch (IOException e) {
      LOG.error("Failed to handle CREATED_INVENTORY_INSTANCE event {}", e, eventContent);
      return Future.failedFuture(e);
    }
  }

  /**
   * Adds specified instanceId to record and additional custom field with instanceId to parsed record.
   * Updates changed record in database.
   *
   * @param record      record to update
   * @param instanceId  instance id
   * @param tenantId    tenant id
   * @return  future with updated parsed record
   */
  public Future<ParsedRecord> setInstanceIdToRecord(Record record, String instanceId, String tenantId) {
    if (record.getExternalIdsHolder() == null) {
      record.setExternalIdsHolder(new ExternalIdsHolder());
    }
    record.getExternalIdsHolder().setInstanceId(instanceId);
    AdditionalFieldsUtil.addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    return recordDao.updateParsedRecord(record, tenantId);
  }
}
