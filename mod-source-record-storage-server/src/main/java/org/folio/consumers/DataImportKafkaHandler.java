package org.folio.consumers;

import static java.lang.String.format;
import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.ERROR;
import static org.apache.logging.log4j.Level.INFO;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.dataimport.util.DataImportContextKeys.JOB_PROFILE_SNAPSHOT_ID;
import static org.folio.dataimport.util.DataImportHeaders.CHUNK_ID;
import static org.folio.dataimport.util.DataImportHeaders.JOB_EXECUTION_ID;
import static org.folio.dataimport.util.DataImportHeaders.RECORD_ID;
import static org.folio.dataimport.util.DataImportHeaders.USER_ID;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.io.IOException;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.events.EventManager;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.folio.services.caches.JobProfileSnapshotCache;
import org.folio.services.util.RestUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, byte[]> {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String LOG_MESSAGE_TEMPLATE =
    "%s for jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' and userId: '%s'";
  private static final Set<String> CANCELLED_JOB_ALLOWED_EVENTS = Set.of(
    DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING.value());

  private final JobProfileSnapshotCache profileSnapshotCache;
  private final CancelledJobsIdsCache cancelledJobsIdCache;
  private final DiErrorEventSender diErrorEventSender;

  @Autowired
  public DataImportKafkaHandler(JobProfileSnapshotCache profileSnapshotCache,
                                CancelledJobsIdsCache cancelledJobsIdCache,
                                DiErrorEventSender diErrorEventSender) {
    this.profileSnapshotCache = profileSnapshotCache;
    this.cancelledJobsIdCache = cancelledJobsIdCache;
    this.diErrorEventSender = diErrorEventSender;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> targetRecord) {
    LOGGER.trace("handle:: Handling kafka record: {}", targetRecord);
    String recordId = extractHeaderValue(RECORD_ID, targetRecord.headers());
    String chunkId = extractHeaderValue(CHUNK_ID, targetRecord.headers());
    String userId = extractHeaderValue(USER_ID, targetRecord.headers());
    String jobId = extractHeaderValue(JOB_EXECUTION_ID, targetRecord.headers());
    try {
      DataImportEventPayload eventPayload = decodePayload(targetRecord);

      if (isSkippedAsCancelled(jobId, eventPayload, targetRecord)) {
        return Future.succeededFuture(targetRecord.key());
      }

      String[] debugInfo = {eventPayload.getJobExecutionId(), recordId, chunkId, userId};
      printLogInfo(Level.INFO,
        format("handle:: Data import event payload has been received with event type: %s", eventPayload.getEventType()),
        debugInfo);

      populateContext(eventPayload, targetRecord, recordId, chunkId, userId);

      return resolveSnapshot(eventPayload, debugInfo)
        .compose(snapshot -> processEvent(eventPayload, snapshot, debugInfo))
        .compose(processedPayload -> toResult(processedPayload, targetRecord, debugInfo));
    } catch (Exception e) {
      LOGGER.error(
        "handle:: Failed to process data import kafka record from topic '{}' with recordId: '{}' and chunkId: '{}' ",
        targetRecord.topic(), recordId, chunkId, e);
      return Future.failedFuture(e);
    }
  }

  private DataImportEventPayload decodePayload(KafkaConsumerRecord<String, byte[]> targetRecord) throws IOException {
    return Json.decodeValue(
      DatabindCodec.mapper().readValue(targetRecord.value(), Event.class).getEventPayload(),
      DataImportEventPayload.class);
  }

  private boolean isSkippedAsCancelled(String jobId, DataImportEventPayload eventPayload,
                                       KafkaConsumerRecord<String, byte[]> targetRecord) {
    if (cancelledJobsIdCache.contains(jobId) && !CANCELLED_JOB_ALLOWED_EVENTS.contains(eventPayload.getEventType())) {
      LOGGER.info("handle:: Skipping processing of event, topic: '{}', jobExecutionId: '{}', " +
                  "eventType: '{}' because the job has been cancelled", targetRecord.topic(), jobId,
        eventPayload.getEventType());
      return true;
    }
    return false;
  }

  private void populateContext(DataImportEventPayload eventPayload, KafkaConsumerRecord<String, byte[]> targetRecord,
                               String recordId, String chunkId, String userId) {
    eventPayload.getContext().put(RECORD_ID, recordId);
    eventPayload.getContext().put(CHUNK_ID, chunkId);
    eventPayload.getContext().put(USER_ID, userId);
    populateWithCommonHeaders(eventPayload, targetRecord);
  }

  /**
   * Obtains the job profile snapshot. Any failure (snapshot not found or loading error) is logged,
   * results in a DI_ERROR event and a failed future.
   */
  private Future<ProfileSnapshotWrapper> resolveSnapshot(DataImportEventPayload eventPayload, String[] debugInfo) {
    ConnectionParams params = RestUtil.retrieveOkapiConnectionParams(eventPayload);
    String jobProfileSnapshotId = eventPayload.getContext().get(JOB_PROFILE_SNAPSHOT_ID);
    printLogInfo(DEBUG, format("handle:: Using jobProfileSnapshotId: %s", jobProfileSnapshotId), debugInfo);

    return profileSnapshotCache.get(jobProfileSnapshotId, params)
      .onFailure(e -> {
        printLogInfo(ERROR, format("handle:: Failed to get job profile snapshot with id '%s'", jobProfileSnapshotId), e,
          debugInfo);
        diErrorEventSender.send(eventPayload);
      });
  }

  private Future<DataImportEventPayload> processEvent(DataImportEventPayload eventPayload,
                                                      ProfileSnapshotWrapper snapshot, String[] debugInfo) {
    printLogInfo(DEBUG, "handle:: Handle event by EventManager", debugInfo);
    return Future.fromCompletionStage(EventManager.handleEvent(eventPayload, snapshot))
      .onFailure(e -> printLogInfo(ERROR, "handle:: Failed to process data import event payload", e, debugInfo));
  }

  private Future<String> toResult(DataImportEventPayload processedPayload,
                                  KafkaConsumerRecord<String, byte[]> targetRecord, String[] debugInfo) {
    if (DI_ERROR.value().equals(processedPayload.getEventType())) {
      printLogInfo(ERROR,
        format("handle:: Failed to process data import event payload from topic %s", targetRecord.topic()), debugInfo);
      return Future.failedFuture(format(
        "Failed to process data import event payload from topic '%s' by jobExecutionId: '%s' with recordId: '%s' and chunkId: '%s' ",
        targetRecord.topic(), debugInfo[0], debugInfo[1], debugInfo[2]));
    }
    printLogInfo(INFO,
      format("handle:: Successfully processed data import event payload from topic %s", targetRecord.topic()),
      debugInfo);
    return Future.succeededFuture(targetRecord.key());
  }

  private static void printLogInfo(Level level, String message, String... args) {
    printLogInfo(level, message, null, args);
  }

  private static void printLogInfo(Level level, String message, Throwable tr, String... args) {
    String formattedMessage = format(LOG_MESSAGE_TEMPLATE, message, args[0], args[1], args[2], args[3]);
    if (tr != null) {
      LOGGER.log(level, formattedMessage, tr);
    } else {
      LOGGER.log(level, formattedMessage);
    }
  }

  private void populateWithCommonHeaders(DataImportEventPayload eventPayload,
                                         KafkaConsumerRecord<String, byte[]> kafkaRecord) {
    String permissions = extractHeaderValue(PERMISSIONS, kafkaRecord.headers());
    if (StringUtils.isNotBlank(permissions)) {
      eventPayload.getContext().put(PERMISSIONS, permissions);
    }
    String requestId = extractHeaderValue(XOkapiHeaders.REQUEST_ID, kafkaRecord.headers());
    if (StringUtils.isNotBlank(requestId)) {
      eventPayload.getContext().put(XOkapiHeaders.REQUEST_ID, requestId);
    }
    String userId = extractHeaderValue(XOkapiHeaders.USER_ID, kafkaRecord.headers());
    if (StringUtils.isNotBlank(userId)) {
      eventPayload.getContext().put(XOkapiHeaders.USER_ID, userId);
    }
  }
}
