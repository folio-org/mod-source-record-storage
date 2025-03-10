package org.folio.services.handlers;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.folio.dao.util.MarcUtil.reorderMarcRecordFields;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByNotSnapshotId;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.fillHrIdFieldInMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.services.util.AdditionalFieldsUtil.isFieldsFillingNeeded;
import static org.folio.services.util.AdditionalFieldsUtil.remove035WithActualHrId;
import static org.folio.services.util.AdditionalFieldsUtil.updateLatestTransactionDate;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;
import static org.folio.services.util.EventHandlingUtil.toOkapiHeaders;
import static org.folio.services.util.RestUtil.retrieveOkapiConnectionParams;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.kafka.KafkaConfig;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.exceptions.PostProcessingException;
import org.folio.services.util.TypeConnection;
import org.jooq.Condition;

public abstract class AbstractPostProcessingEventHandler implements EventHandler {

  private static final String USER_ID_HEADER = "userId";
  public static final String JOB_EXECUTION_ID_KEY = "JOB_EXECUTION_ID";
  protected static final String HRID_FIELD = "hrid";
  private static final Logger LOG = LogManager.getLogger();
  private static final AtomicInteger indexer = new AtomicInteger();
  private static final String FAIL_MSG = "Failed to handle event {}";
  private static final String EVENT_HAS_NO_DATA_MSG =
    "Failed to handle event, cause event payload context does not contain needed data";
  private static final String MAPPING_PARAMS_NOT_FOUND_MSG = "MappingParameters was not found by jobExecutionId: '%s'";
  private static final String DATA_IMPORT_IDENTIFIER = "DI";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String DISCOVERY_SUPPRESS_FIELD = "discoverySuppress";
  private static final String FAILED_UPDATE_STATE_MSG = "Error during update records state to OLD";
  private static final String ID_FIELD = "id";
  public static final String POST_PROCESSING_INDICATOR = "POST_PROCESSING";
  public static final String CENTRAL_TENANT_INSTANCE_UPDATED_FLAG = "CENTRAL_TENANT_INSTANCE_UPDATED";
  public static final String CENTRAL_TENANT_ID = "CENTRAL_TENANT_ID";
  private final KafkaConfig kafkaConfig;
  private final MappingParametersSnapshotCache mappingParamsCache;
  private final Vertx vertx;
  private final RecordService recordService;
  private final SnapshotService snapshotService;


  protected AbstractPostProcessingEventHandler(RecordService recordService, SnapshotService snapshotService, KafkaConfig kafkaConfig,
                                               MappingParametersSnapshotCache mappingParamsCache, Vertx vertx) {
    this.recordService = recordService;
    this.snapshotService = snapshotService;
    this.kafkaConfig = kafkaConfig;
    this.mappingParamsCache = mappingParamsCache;
    this.vertx = vertx;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    var eventType = dataImportEventPayload.getEventType();
    var jobExecutionId = dataImportEventPayload.getJobExecutionId();
    try {
      mappingParamsCache.get(jobExecutionId, retrieveOkapiConnectionParams(dataImportEventPayload, vertx))
        .compose(parametersOptional -> parametersOptional
          .map(mappingParams -> prepareRecord(dataImportEventPayload, mappingParams))
          .orElse(Future.failedFuture(format(MAPPING_PARAMS_NOT_FOUND_MSG, jobExecutionId))))
        .compose(record -> {
          if (centralTenantOperationExists(dataImportEventPayload)) {
            return saveRecordForCentralTenant(dataImportEventPayload, record, jobExecutionId);
          }
          return saveRecord(record, toOkapiHeaders(dataImportEventPayload));
        })
        .onSuccess(record -> {
          sendReplyEvent(dataImportEventPayload, record);
          sendAdditionalEvent(dataImportEventPayload, record);
          future.complete(dataImportEventPayload);
        })
        .onFailure(throwable -> {
          LOG.warn(FAIL_MSG, eventType, throwable);
          dataImportEventPayload.setEventType(getNextEventType(dataImportEventPayload));
          future.completeExceptionally(throwable);
        });
    } catch (Exception e) {
      LOG.warn(FAIL_MSG, eventType, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    var currentNode = dataImportEventPayload.getCurrentNode();
    if (currentNode != null && MAPPING_PROFILE == currentNode.getContentType()) {
      var mappingProfile = JsonObject.mapFrom(currentNode.getContent()).mapTo(MappingProfile.class);
      return mappingProfile.getExistingRecordType() == getExternalType();
    }
    return false;
  }

  protected abstract void sendAdditionalEvent(DataImportEventPayload dataImportEventPayload, Record record);

  protected abstract String getNextEventType(DataImportEventPayload dataImportEventPayload);

  protected String getEventKey() {
    return String.valueOf(indexer.incrementAndGet() % 100);
  }

  protected abstract DataImportEventTypes replyEventType();

  protected abstract TypeConnection typeConnection();

  protected abstract void prepareEventPayload(DataImportEventPayload dataImportEventPayload);

  protected List<KafkaHeader> getKafkaHeaders(DataImportEventPayload eventPayload) {
    List<KafkaHeader> kafkaHeaders = new ArrayList<>(List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, eventPayload.getOkapiUrl()),
      KafkaHeader.header(OKAPI_TENANT_HEADER, eventPayload.getTenant()),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, eventPayload.getToken()))
    );

    var recordId = eventPayload.getContext().get(RECORD_ID_HEADER);
    var userId = eventPayload.getContext().get(USER_ID_HEADER);
    if (recordId != null) {
      kafkaHeaders.add(KafkaHeader.header(RECORD_ID_HEADER, recordId));
    }
    if (userId != null) {
      kafkaHeaders.add(KafkaHeader.header("x-okapi-user-id", userId));
    }
    return kafkaHeaders;
  }

  protected abstract void setExternalIds(ExternalIdsHolder externalIdsHolder, String externalId, String externalHrid);

  protected abstract String getExternalId(Record record);

  protected abstract String getExternalHrid(Record record);

  protected abstract boolean isHridFillingNeeded();

  protected abstract String extractHrid(Record record, JsonObject externalEntity);

  private Record.RecordType getRecordType() {
    return typeConnection().getRecordType();
  }

  private EntityType getMarcType() {
    return typeConnection().getMarcType();
  }

  private EntityType getExternalType() {
    return typeConnection().getExternalType();
  }

  private RecordType getDbType() {
    return typeConnection().getDbType();
  }

  private Future<Record> prepareRecord(DataImportEventPayload dataImportEventPayload, MappingParameters mappingParameters) {
    Promise<Record> recordPromise = Promise.promise();
    var eventContext = dataImportEventPayload.getContext();
    String entityAsString = eventContext.get(getExternalType().value());
    String recordAsString = eventContext.get(getMarcType().value());
    String userId = (String) dataImportEventPayload.getAdditionalProperties().get(USER_ID_HEADER);

    if (isEmpty(entityAsString) || isEmpty(recordAsString)) {
      LOG.warn(EVENT_HAS_NO_DATA_MSG);
      recordPromise.fail(new EventProcessingException(EVENT_HAS_NO_DATA_MSG));
    } else {
      Record record = Json.decodeValue(recordAsString, Record.class);
      var sourceContent = record.getParsedRecord().getContent().toString();
      setUpdatedBy(record, userId);
      updateLatestTransactionDate(record, mappingParameters);

      JsonObject externalEntity = new JsonObject(entityAsString);
      setExternalIds(record, externalEntity); //operations with 001, 003, 035, 999 fields
      remove035FieldWhenUpdateAndContainsHrId(record, DataImportEventTypes.fromValue(dataImportEventPayload.getEventType()));
      setSuppressFormDiscovery(record, externalEntity.getBoolean(DISCOVERY_SUPPRESS_FIELD, false));
      var targetContent = record.getParsedRecord().getContent().toString();
      var content = reorderMarcRecordFields(sourceContent, targetContent);
      record.getParsedRecord().setContent(content);
      recordPromise.complete(record);
    }
    return recordPromise.future();
  }

  private void remove035FieldWhenUpdateAndContainsHrId(Record record, DataImportEventTypes eventType) {
    if (Record.RecordType.MARC_BIB.equals(record.getRecordType())
      && (DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING == eventType)) {
      String hrId = getValueFromControlledField(record, HR_ID_FROM_FIELD);
      remove035WithActualHrId(record, hrId);
    }
  }

  private String prepareReplyEventPayload(DataImportEventPayload dataImportEventPayload, Record record) {
    record.getParsedRecord().setContent(ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord()));
    HashMap<String, String> context = dataImportEventPayload.getContext();
    context.put(JOB_EXECUTION_ID_KEY, dataImportEventPayload.getJobExecutionId());
    context.put(getRecordType().value(), Json.encode(record));
    context.put(DATA_IMPORT_IDENTIFIER, Boolean.TRUE.toString());
    context.put(getMarcType().value(), Json.encode(record));
    prepareEventPayload(dataImportEventPayload);
    return Json.encode(context);
  }

  private void setSuppressFormDiscovery(Record record, boolean suppressFromDiscovery) {
    AdditionalInfo info = record.getAdditionalInfo();
    if (info != null) {
      info.setSuppressDiscovery(suppressFromDiscovery);
    } else {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(suppressFromDiscovery));
    }
  }

  private Future<Void> updatePreviousRecordsState(String externalId, String snapshotId, Map<String, String> okapiHeaders) {
    Condition condition = filterRecordByNotSnapshotId(snapshotId)
      .and(filterRecordByExternalId(externalId));

    return recordService.getRecords(condition, getDbType(), new ArrayList<>(), 0, 999,
      okapiHeaders.get(OKAPI_TENANT_HEADER)).compose(recordCollection -> {
        Promise<Void> result = Promise.promise();
        @SuppressWarnings("squid:S3740")
        List<Future<Record>> futures = new ArrayList<>();
        recordCollection.getRecords()
          .forEach(record -> futures.add(recordService.updateRecord(record.withState(Record.State.OLD), okapiHeaders)));
        GenericCompositeFuture.all(futures).onComplete(ar -> {
          if (ar.succeeded()) {
            result.complete();
          } else {
            result.fail(ar.cause());
            LOG.warn(FAILED_UPDATE_STATE_MSG, ar.cause());
          }
        });
        return result.future();
      });
  }

  /**
   * Adds specified externalId and externalHrid to record and additional custom field with externalId to parsed record.
   *
   * @param record         record to update
   * @param externalEntity externalEntity in Json
   */
  private void setExternalIds(Record record, JsonObject externalEntity) {
    if (record.getExternalIdsHolder() == null) {
      record.setExternalIdsHolder(new ExternalIdsHolder());
    }
    if (isNotEmpty(getExternalId(record)) || isNotEmpty(getExternalHrid(record))) {
      if (isFieldsFillingNeeded(record, externalEntity)) {
        executeHridManipulation(record, externalEntity);
      }
    } else {
      executeHridManipulation(record, externalEntity);
    }
  }

  private void executeHridManipulation(Record record, JsonObject externalEntity) {
    var externalId = externalEntity.getString(ID_FIELD);
    var externalHrid = extractHrid(record, externalEntity);
    var externalIdsHolder = record.getExternalIdsHolder();
    setExternalIds(externalIdsHolder, externalId, externalHrid);
    boolean isAddedField = addFieldToMarcRecord(record, TAG_999, 'i', externalId);
    if (isHridFillingNeeded()) {
      fillHrIdFieldInMarcRecord(Pair.of(record, externalEntity));
    }
    if (!isAddedField) {
      throw new PostProcessingException(
        format("Failed to add externalEntity id '%s' to record with id '%s'", externalId, record.getId()));
    }
  }

  /**
   * Updates specific record. If it doesn't exist - then just save it.
   *
   * @param record   - target record
   * @param okapiHeaders - okapi headers
   * @return - Future with Record result
   */
  private Future<Record> saveRecord(Record record, Map<String, String> okapiHeaders) {
    var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
    return recordService.getRecordById(record.getId(), tenantId)
      .compose(r -> {
        if (r.isPresent()) {
          return recordService.updateParsedRecord(record, okapiHeaders).map(record.withGeneration(r.get().getGeneration()));
        } else {
          record.getRawRecord().setId(record.getId());
          return recordService.saveRecord(record, okapiHeaders).map(record);
        }
      })
      .compose(updatedRecord ->
        updatePreviousRecordsState(getExternalId(updatedRecord), updatedRecord.getSnapshotId(), okapiHeaders)
          .map(updatedRecord)
      );
  }

  private void sendReplyEvent(DataImportEventPayload dataImportEventPayload, Record record) {
    var replyEventType = replyEventType();
    if (replyEventType != null) {
      var key = getEventKey();
      var kafkaHeaders = getKafkaHeaders(dataImportEventPayload);
      String replyEventPayload = prepareReplyEventPayload(dataImportEventPayload, record);
      sendEventToKafka(dataImportEventPayload.getTenant(), replyEventPayload, replyEventType.value(),
        kafkaHeaders, kafkaConfig, key);
    }
  }

  private static boolean centralTenantOperationExists(DataImportEventPayload dataImportEventPayload) {
    return dataImportEventPayload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG) != null &&
      dataImportEventPayload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG).equals("true");
  }

  private Future<Record> saveRecordForCentralTenant(DataImportEventPayload dataImportEventPayload, Record
    record, String jobExecutionId) {
    String centralTenantId = dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID);
    dataImportEventPayload.getContext().remove(CENTRAL_TENANT_INSTANCE_UPDATED_FLAG);
    LOG.info("handle:: Processing AbstractPostProcessingEventHandler - saving record by jobExecutionId: {} for the central tenantId: {}", jobExecutionId, centralTenantId);
    var okapiHeaders = toOkapiHeaders(dataImportEventPayload);
    if (centralTenantId != null) {
      okapiHeaders.put(OKAPI_TENANT_HEADER, centralTenantId);
      return snapshotService.copySnapshotToOtherTenant(record.getSnapshotId(), dataImportEventPayload.getTenant(), centralTenantId)
        .compose(f -> saveRecord(record, okapiHeaders));
    }
    else {
      return saveRecord(record, okapiHeaders);
    }
  }

  private void setUpdatedBy(Record changedRecord, String userId) {
    if (changedRecord.getMetadata() != null) {
      changedRecord.getMetadata().setUpdatedByUserId(userId);
    } else {
      changedRecord.withMetadata(new Metadata().withUpdatedByUserId(userId));
    }
  }
}
