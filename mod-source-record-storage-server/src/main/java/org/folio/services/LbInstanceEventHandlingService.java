package org.folio.services;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.folio.dao.util.LbRecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.LbRecordDaoUtil.filterRecordByNotSnapshotId;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.dao.LbRecordDao;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.util.AdditionalFieldsUtil;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Component
public class LbInstanceEventHandlingService implements LbEventHandlingService {

  private static final Logger LOG = LoggerFactory.getLogger(LbInstanceEventHandlingService.class);

  private static final String FAIL_MSG = "Failed to handle instance event {}";

  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle Instance event, cause event payload context does not contain INSTANCE and/or MARC_BIBLIOGRAPHIC data";

  private final LbRecordDao recordDao;

  @Autowired
  public LbInstanceEventHandlingService(final LbRecordDao recordDao) {
    this.recordDao = recordDao;
  }

  /**
   * Handles DI_INVENTORY_INSTANCE_CREATED or DI_INVENTORY_INSTANCE_UPDATED event
   *
   * {@inheritDoc}
   */
  @Override
  public Future<Boolean> handleEvent(String eventContent, OkapiConnectionParams params) {
    try {
      Pair<String, String> instanceRecordPair = extractPayload(eventContent);
      if (StringUtils.isEmpty(instanceRecordPair.getLeft()) || StringUtils.isEmpty(instanceRecordPair.getRight())) {
        LOG.error(EVENT_HAS_NO_DATA_MSG);
        return Future.failedFuture(EVENT_HAS_NO_DATA_MSG);
      }
      String tenantId = params.getTenantId();
      return setInstanceIdToRecord(
        ObjectMapperTool.getMapper().readValue(instanceRecordPair.getRight(), Record.class),
        new JsonObject(instanceRecordPair.getLeft()), tenantId)
        .compose(record -> updatePreviousRecords(record.getExternalIdsHolder().getInstanceId(), record.getSnapshotId(), tenantId))
        .map(true);
    } catch (IOException e) {
      LOG.error(FAIL_MSG, e, eventContent);
      return Future.failedFuture(e);
    }
  }

  private Pair<String, String> extractPayload(String eventContent) throws IOException {
    DataImportEventPayload dataImportEventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(eventContent), DataImportEventPayload.class);
    String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
    String recordAsString = dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    return Pair.of(instanceAsString, recordAsString);
  }

  private Future<Void> updatePreviousRecords(String instanceId, String snapshotId, String tenantId) {
    Condition condition = filterRecordByNotSnapshotId(snapshotId)
      .and(filterRecordByInstanceId(instanceId));
    return recordDao.getRecords(condition, new ArrayList<>(), 0, 999, tenantId)
      .compose(recordCollection -> {
        Promise<Void> result = Promise.promise();
        @SuppressWarnings("squid:S3740")
        List<Future> futures = new ArrayList<>();
        recordCollection.getRecords()
          .forEach(record -> futures.add(recordDao.updateRecord(record.withState(Record.State.OLD), tenantId)));
        CompositeFuture.all(futures).onComplete(ar -> {
          if (ar.succeeded()) {
            result.complete();
          } else {
            result.fail(ar.cause());
            LOG.error(ar.cause(), "ERROR during update old records state for instance chane event");
          }
        });
        return result.future();
      });
  }

  /**
   * Adds specified instanceId to record and additional custom field with instanceId to parsed record.
   * Updates changed record in database.
   *
   * @param record   record to update
   * @param instance instance in Json
   * @param tenantId tenant id
   * @return future with updated record
   */
  private Future<Record> setInstanceIdToRecord(Record record, JsonObject instance, String tenantId) {
    if (record.getExternalIdsHolder() == null) {
      record.setExternalIdsHolder(new ExternalIdsHolder());
    }
    if (isNotEmpty(record.getExternalIdsHolder().getInstanceId())) {
      return Future.succeededFuture(record);
    }
    String instanceId = instance.getString("id");
    boolean isAddedField = AdditionalFieldsUtil.addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(Pair.of(record, instance));
    if (isAddedField) {
      record.getExternalIdsHolder().setInstanceId(instanceId);
      return recordDao.updateParsedRecord(record, tenantId).map(record);
    }
    return Future.failedFuture(new RuntimeException(format("Failed to add instance id '%s' to record with id '%s'", instanceId, record.getId())));
  }

}
