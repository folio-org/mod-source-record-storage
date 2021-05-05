package org.folio.services.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.dao.RecordDao;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.kafka.KafkaConfig;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.exceptions.PostProcessingException;
import org.folio.services.util.AdditionalFieldsUtil;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByNotSnapshotId;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_INSTANCE_HRID_SET;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Component
public class InstancePostProcessingEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private static final String FAIL_MSG = "Failed to handle instance event {}";
  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle Instance event, cause event payload context does not contain INSTANCE and/or MARC_BIBLIOGRAPHIC data";
  private static final String DATA_IMPORT_IDENTIFIER = "DI";
  private static final String CORRELATION_ID_HEADER = "correlationId";

  private final RecordDao recordDao;
  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;

  @Autowired
  public InstancePostProcessingEventHandler(final RecordDao recordDao, Vertx vertx, KafkaConfig kafkaConfig) {
    this.recordDao = recordDao;
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  /**
   * Handles DI_INVENTORY_INSTANCE_CREATED or DI_INVENTORY_INSTANCE_UPDATED event
   * <p>
   * {@inheritDoc}
   */
  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
      String recordAsString = dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
      if (StringUtils.isEmpty(instanceAsString) || StringUtils.isEmpty(recordAsString)) {
        LOG.error(EVENT_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(EVENT_HAS_NO_DATA_MSG));
        return future;
      }

      String tenantId = dataImportEventPayload.getTenant();
      Record record = new ObjectMapper().readValue(recordAsString, Record.class);
      AdditionalFieldsUtil.updateLatestTransactionDate(record, dataImportEventPayload.getContext());

      JsonObject instance = new JsonObject(instanceAsString);
      setInstanceIdToRecord(record, instance);
      setSuppressFormDiscovery(record, instance.getBoolean("discoverySuppress", false));
      insertOrUpdateRecordWithExternalIdsHolder(record, tenantId)
        .compose(updatedRecord -> updatePreviousRecords(updatedRecord.getExternalIdsHolder().getInstanceId(), updatedRecord.getSnapshotId(), tenantId)
          .map(updatedRecord))
        .onComplete(updateAr -> {
          if (updateAr.succeeded()) {
            record.getParsedRecord().setContent(ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord()));
            HashMap<String, String> context = dataImportEventPayload.getContext();
            context.put(Record.RecordType.MARC_BIB.value(), Json.encode(record));
            context.put(DATA_IMPORT_IDENTIFIER, "true");
            List<KafkaHeader> kafkaHeaders = getKafkaHeaders(dataImportEventPayload);
            String key = String.valueOf(indexer.incrementAndGet() % 100);
            context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
            sendEventToKafka(dataImportEventPayload.getTenant(), Json.encode(context), DI_SRS_MARC_BIB_INSTANCE_HRID_SET.value(),
              kafkaHeaders, kafkaConfig, key);
            // MODSOURMAN-384: sent event to log when record updated implicitly only for INSTANCE_UPDATED case
            if (dataImportEventPayload.getEventType().equals(DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value())) {
              dataImportEventPayload.setEventType(DI_SRS_MARC_BIB_RECORD_UPDATED.value());
              sendEventToKafka(dataImportEventPayload.getTenant(), Json.encode(dataImportEventPayload), DI_SRS_MARC_BIB_RECORD_UPDATED.value(),
                kafkaHeaders, kafkaConfig, key);
            }
            future.complete(dataImportEventPayload);
          } else {
            LOG.error(FAIL_MSG, updateAr.cause());
            future.completeExceptionally(updateAr.cause());
          }
        });
    } catch (Exception e) {
      LOG.error(FAIL_MSG, e, dataImportEventPayload);
      future.completeExceptionally(e);
    }
    return future;
  }

  private void setSuppressFormDiscovery(Record record, boolean suppressFromDiscovery) {
    AdditionalInfo info = record.getAdditionalInfo();
    if (info != null) {
      info.setSuppressDiscovery(suppressFromDiscovery);
    } else {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(suppressFromDiscovery));
    }
  }

  private List<KafkaHeader> getKafkaHeaders(DataImportEventPayload eventPayload) {
    List<KafkaHeader> kafkaHeaders = new ArrayList<>(List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, eventPayload.getOkapiUrl()),
      KafkaHeader.header(OKAPI_TENANT_HEADER, eventPayload.getTenant()),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, eventPayload.getToken())));

    String correlationId = eventPayload.getContext().get(CORRELATION_ID_HEADER);
    if (correlationId != null) {
      kafkaHeaders.add(KafkaHeader.header(CORRELATION_ID_HEADER, correlationId));
    }
    return kafkaHeaders;
  }

  private Future<Void> updatePreviousRecords(String instanceId, String snapshotId, String tenantId) {
    Condition condition = filterRecordByNotSnapshotId(snapshotId)
      .and(filterRecordByInstanceId(instanceId));

    return recordDao.getRecords(condition, RecordType.MARC_BIB, new ArrayList<>(), 0, 999, tenantId)
      .compose(recordCollection -> {
        Promise<Void> result = Promise.promise();
        @SuppressWarnings("squid:S3740")
        List<Future<Record>> futures = new ArrayList<>();
        recordCollection.getRecords()
          .forEach(record -> futures.add(recordDao.updateRecord(record.withState(Record.State.OLD), tenantId)));
        GenericCompositeFuture.all(futures).onComplete(ar -> {
          if (ar.succeeded()) {
            result.complete();
          } else {
            result.fail(ar.cause());
            LOG.error( "ERROR during update old records state for instance chane event", ar.cause());
          }
        });
        return result.future();
      });
  }

  /**
   * Adds specified instanceId and instanceHrid to record and additional custom field with instanceId to parsed record.
   *
   * @param record   record to update
   * @param instance instance in Json
   */
  private void setInstanceIdToRecord(Record record, JsonObject instance) {
    if (record.getExternalIdsHolder() == null) {
      record.setExternalIdsHolder(new ExternalIdsHolder());
    }
    if (isNotEmpty(record.getExternalIdsHolder().getInstanceId())
      || isNotEmpty(record.getExternalIdsHolder().getInstanceHrid())) {
      return;
    }
    String instanceId = instance.getString("id");
    String instanceHrid = instance.getString("hrid");
    record.getExternalIdsHolder().setInstanceHrid(instanceHrid);
    boolean isAddedField = AdditionalFieldsUtil.addFieldToMarcRecord(record, TAG_999, 'i', instanceId);
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(Pair.of(record, instance));
    if (!isAddedField) {
      throw new PostProcessingException(format("Failed to add instance id '%s' to record with id '%s'", instanceId, record.getId()));
    }
    record.getExternalIdsHolder().setInstanceId(instanceId);
  }

  /**
   * Updates specific record. If it doesn't exist - then just save it.
   *
   * @param record   - target record
   * @param tenantId - tenantId
   * @return - Future with Record result
   */
  private Future<Record> insertOrUpdateRecordWithExternalIdsHolder(Record record, String tenantId) {
    return recordDao.getRecordById(record.getId(), tenantId)
      .compose(r -> {
        if (r.isPresent()) {
          return recordDao.updateParsedRecord(record, tenantId).map(record);
        } else {
          record.getRawRecord().setId(record.getId());
          return recordDao.saveRecord(record, tenantId).map(record);
        }
      });
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MAPPING_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MappingProfile mappingProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MappingProfile.class);
      return mappingProfile.getExistingRecordType() == EntityType.INSTANCE;
    }
    return false;
  }
}
