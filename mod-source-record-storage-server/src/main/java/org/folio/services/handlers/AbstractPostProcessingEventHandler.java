package org.folio.services.handlers;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isEmpty;
import static org.apache.commons.lang.StringUtils.isNotEmpty;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByNotSnapshotId;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.fillHrIdFieldInMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.isFieldsFillingNeeded;
import static org.folio.services.util.AdditionalFieldsUtil.updateLatestTransactionDate;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;
import static org.folio.services.util.RestUtil.retrieveOkapiConnectionParams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.services.RecordService;
import org.jooq.Condition;

import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.dao.RecordDao;
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
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.exceptions.PostProcessingException;
import org.folio.services.util.TypeConnection;

public abstract class AbstractPostProcessingEventHandler implements EventHandler {

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
  private final RecordDao recordDao;
  private final KafkaConfig kafkaConfig;
  private final MappingParametersSnapshotCache mappingParamsCache;
  private final Vertx vertx;
  private final RecordService recordService;

  public AbstractPostProcessingEventHandler(final RecordDao recordDao, KafkaConfig kafkaConfig, RecordService recordService,
                                            MappingParametersSnapshotCache mappingParamsCache, Vertx vertx) {
    this.recordDao = recordDao;
    this.kafkaConfig = kafkaConfig;
    this.mappingParamsCache = mappingParamsCache;
    this.vertx = vertx;
    this.recordService = recordService;
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
        .compose(record -> saveRecord(record, dataImportEventPayload.getTenant()))
        .onSuccess(record -> {
          sendReplyEvent(dataImportEventPayload, record);
          sendAdditionalEvent(dataImportEventPayload, record);
          future.complete(dataImportEventPayload);
        })
        .onFailure(throwable -> {
          LOG.error(FAIL_MSG, eventType, throwable);
          future.completeExceptionally(throwable);
        });
    } catch (Exception e) {
      LOG.error(FAIL_MSG, eventType, e);
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

  protected String getEventKey() {
    return String.valueOf(indexer.incrementAndGet() % 100);
  }

  protected abstract DataImportEventTypes replyEventType();

  protected abstract TypeConnection typeConnection();

  protected List<KafkaHeader> getKafkaHeaders(DataImportEventPayload eventPayload) {
    List<KafkaHeader> kafkaHeaders = new ArrayList<>(List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, eventPayload.getOkapiUrl()),
      KafkaHeader.header(OKAPI_TENANT_HEADER, eventPayload.getTenant()),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, eventPayload.getToken()))
    );

    String recordId = eventPayload.getContext().get(RECORD_ID_HEADER);
    if (recordId != null) {
      kafkaHeaders.add(KafkaHeader.header(RECORD_ID_HEADER, recordId));
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
    if (isEmpty(entityAsString) || isEmpty(recordAsString)) {
      LOG.error(EVENT_HAS_NO_DATA_MSG);
      recordPromise.fail(new EventProcessingException(EVENT_HAS_NO_DATA_MSG));
    } else {
      Record record = Json.decodeValue(recordAsString, Record.class);
      updateLatestTransactionDate(record, mappingParameters);

      JsonObject externalEntity = new JsonObject(entityAsString);
      setExternalIds(record, externalEntity);
      setSuppressFormDiscovery(record, externalEntity.getBoolean(DISCOVERY_SUPPRESS_FIELD, false));
      recordPromise.complete(record);
    }
    return recordPromise.future();
  }

  private String prepareReplyEventPayload(DataImportEventPayload dataImportEventPayload, Record record) {
    record.getParsedRecord().setContent(ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord()));
    HashMap<String, String> context = dataImportEventPayload.getContext();
    context.put(JOB_EXECUTION_ID_KEY, dataImportEventPayload.getJobExecutionId());
    context.put(getRecordType().value(), Json.encode(record));
    context.put(DATA_IMPORT_IDENTIFIER, Boolean.TRUE.toString());
    context.put(getMarcType().value(), Json.encode(record));
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

  private Future<Void> updatePreviousRecordsState(String externalId, String snapshotId, String tenantId) {
    Condition condition = filterRecordByNotSnapshotId(snapshotId)
      .and(filterRecordByExternalId(externalId));

    return recordDao.getRecords(condition, getDbType(), new ArrayList<>(), 0, 999, tenantId)
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
            LOG.error(FAILED_UPDATE_STATE_MSG, ar.cause());
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
   * @param tenantId - tenantId
   * @return - Future with Record result
   */
  private Future<Record> saveRecord(Record record, String tenantId) {
    return recordDao.getRecordById(record.getId(), tenantId)
      .compose(r -> {
        if (r.isPresent()) {
          return recordDao.updateParsedRecord(record, tenantId).map(record.withGeneration(r.get().getGeneration()));
        } else {
          record.getRawRecord().setId(record.getId());
          return recordService.saveRecord(record, tenantId).map(record);
        }
      })
      .compose(updatedRecord ->
        updatePreviousRecordsState(getExternalId(updatedRecord), updatedRecord.getSnapshotId(), tenantId)
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

}
