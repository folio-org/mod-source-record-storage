package org.folio.services.handlers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.dao.RecordDao;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.*;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.exceptions.PostProcessingException;
import org.folio.services.util.AdditionalFieldsUtil;
import org.jooq.Condition;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.VariableField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByNotSnapshotId;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.EventHandlingUtil.sendEventWithPayload;

@Component
public class InstancePostProcessingEventHandler implements EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(InstancePostProcessingEventHandler.class);

  private static final String FAIL_MSG = "Failed to handle instance event {}";
  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle Instance event, cause event payload context does not contain INSTANCE and/or MARC_BIBLIOGRAPHIC data";
  private static final String RECORD_UPDATED_EVENT_TYPE = "DI_SRS_MARC_BIB_INSTANCE_HRID_SET";
  private static final String DATA_IMPORT_IDENTIFIER = "DI";
  private static final String ANY_STRING = "*";

  private final RecordDao recordDao;
  private final Vertx vertx;

  @Autowired
  public InstancePostProcessingEventHandler(final RecordDao recordDao, Vertx vertx) {
    this.recordDao = recordDao;
    this.vertx = vertx;
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
      updateLatestTransactionDate(record, dataImportEventPayload.getContext());

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
            context.put(Record.RecordType.MARC.value(), Json.encode(record));
            context.put(DATA_IMPORT_IDENTIFIER, "true");
            OkapiConnectionParams params = getConnectionParams(dataImportEventPayload);
            sendEventWithPayload(Json.encode(context), RECORD_UPDATED_EVENT_TYPE, params);

            context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
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

  private OkapiConnectionParams getConnectionParams(DataImportEventPayload dataImportEventPayload) {
    OkapiConnectionParams params = new OkapiConnectionParams();
    params.setOkapiUrl(dataImportEventPayload.getOkapiUrl());
    params.setTenantId(dataImportEventPayload.getTenant());
    params.setToken(dataImportEventPayload.getToken());
    params.setVertx(vertx);
    return params;
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

  private void updateLatestTransactionDate(Record record, HashMap<String, String> context) throws JsonProcessingException {
    if (isField005NeedToUpdate(record, context)) {
      String date = AdditionalFieldsUtil.dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
      boolean isLatestTransactionDateUpdated = AdditionalFieldsUtil.addControlledFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_005, date, true);
      if (!isLatestTransactionDateUpdated) {
        throw new PostProcessingException(format("Failed to update field '005' to record with id '%s'", record.getId()));
      }
    }
  }

  private boolean isField005NeedToUpdate(Record record, HashMap<String, String> context) throws JsonProcessingException {
    boolean needToUpdate = true;
    List<MarcFieldProtectionSetting> fieldProtectionSettings = getFieldProtectionSettings(context);
    if ((fieldProtectionSettings != null) && !fieldProtectionSettings.isEmpty()) {
      MarcReader reader = new MarcJsonReader(new ByteArrayInputStream(record.getParsedRecord().getContent().toString().getBytes()));
      if (reader.hasNext()) {
        org.marc4j.marc.Record marcRecord = reader.next();
        for (VariableField field : marcRecord.getVariableFields(AdditionalFieldsUtil.TAG_005)) {
          needToUpdate = isNotProtected(fieldProtectionSettings, (ControlField) field);
          break;
        }
      }
    }
    return needToUpdate;
  }

  private List<MarcFieldProtectionSetting> getFieldProtectionSettings(HashMap<String, String> context) throws JsonProcessingException {
    List<MarcFieldProtectionSetting> fieldProtectionSettings = new ArrayList<>();
    if (isNotBlank(context.get("MAPPING_PARAMS"))) {
      MappingParameters mappingParameters = (new ObjectMapper()).readValue(context.get("MAPPING_PARAMS"), MappingParameters.class);
      fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
    }
    return fieldProtectionSettings;
  }

  private boolean isNotProtected(List<MarcFieldProtectionSetting> fieldProtectionSettings, ControlField field) {
    return fieldProtectionSettings.stream()
      .filter(setting -> setting.getField().equals(ANY_STRING) || setting.getField().equals(field.getTag()))
      .noneMatch(setting -> setting.getData().equals(ANY_STRING) || setting.getData().equals(field.getData()));
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
