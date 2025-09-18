package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.folio.services.caches.JobProfileSnapshotCache;
import org.folio.services.util.RestUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.logging.log4j.Level.DEBUG;
import static org.apache.logging.log4j.Level.ERROR;
import static org.apache.logging.log4j.Level.INFO;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, byte[]> {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String LOG_MESSAGE_TEMPLATE = "%s for jobExecutionId: '%s' and recordId: '%s' and chunkId: '%s' and userId: '%s'";

  public static final String PROFILE_SNAPSHOT_ID_KEY = "JOB_PROFILE_SNAPSHOT_ID";
  static final String RECORD_ID_HEADER = "recordId";
  static final String CHUNK_ID_HEADER = "chunkId";
  static final String USER_ID_HEADER = "userId";

  private Vertx vertx;
  private KafkaConfig kafkaConfig;
  private JobProfileSnapshotCache profileSnapshotCache;
  private CancelledJobsIdsCache cancelledJobsIdCache;

  @Autowired
  public DataImportKafkaHandler(Vertx vertx, JobProfileSnapshotCache profileSnapshotCache,
                                CancelledJobsIdsCache cancelledJobsIdCache, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.profileSnapshotCache = profileSnapshotCache;
    this.cancelledJobsIdCache = cancelledJobsIdCache;
    this.kafkaConfig = kafkaConfig;
  }

  // TODO: possible wrong placement of entity logs when cache error
  private void sendPayloadWithDiError(DataImportEventPayload eventPayload) {
    int retryCount = 0;
    final int maxRetryCount = 3;
    long retryDelay = 1000;

    eventPayload.setEventType(DI_ERROR.value());
    vertx.setTimer(retryDelay, timerId -> sendEventWithRetry(eventPayload, retryCount, maxRetryCount, retryDelay));
  }

  private void sendEventWithRetry(DataImportEventPayload eventPayload, int retryCount, int maxRetryCount, long retryDelay) {
    String eventType = eventPayload.getEventType();
    String jobExecutionId = eventPayload.getJobExecutionId();
    String recordId = eventPayload.getContext().get(RECORD_ID_HEADER);
    LOGGER.info("sendEventWithRetry:: Sending event with type '{}' to kafka topic '{}' with jobExecutionId: '{}' and recordId: '{}'",
      DI_ERROR.value(), kafkaConfig.getEnvId(), eventPayload.getJobExecutionId(), recordId);
    try (var eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      eventPublisher.publish(eventPayload)
        .whenComplete((event, throwable) -> {
          if (throwable != null) {
            if (retryCount < maxRetryCount) {
              LOGGER.info("sendPayloadWithDiError:: Error sending event with type '{}' with jobExecutionId: '{}' and recordId: '{}', retry attempt {}: {}",
                eventType, recordId, jobExecutionId,retryCount + 1, throwable.getCause().getMessage());
              vertx.setTimer(retryDelay, nextTimerId -> sendEventWithRetry(eventPayload, retryCount + 1, maxRetryCount, retryDelay));
            } else {
              LOGGER.error("sendPayloadWithDiError:: Failed to send event with type '{}' with jobExecutionId: '{}' and recordId: '{}' after {} attempts: {}",
                eventType, recordId, jobExecutionId, maxRetryCount, throwable.getCause().getMessage());
            }
          } else {
            LOGGER.info("sendPayloadWithDiError:: Successfully sent event with type '{}' with jobExecutionId: '{}' and recordId: '{}'",
              eventType, jobExecutionId, recordId);
          }
        });
    } catch (Exception e) {
      LOGGER.error("sendPayloadWithDiError:: Error creating Kafka publisher: {}", e.getMessage());
    }
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> targetRecord) {
    LOGGER.trace("handle:: Handling kafka record: {}", targetRecord);
    String recordId = extractHeaderValue(RECORD_ID_HEADER, targetRecord.headers());
    String chunkId = extractHeaderValue(CHUNK_ID_HEADER, targetRecord.headers());
    String userId = extractHeaderValue(USER_ID_HEADER, targetRecord.headers());
    try {
      Promise<String> promise = Promise.promise();
      Event event = DatabindCodec.mapper().readValue(targetRecord.value(), Event.class);
      DataImportEventPayload eventPayload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);
      String jobExecutionId = eventPayload.getJobExecutionId();
      String[] debugInfo = {jobExecutionId, recordId, chunkId, userId};
      printLogInfo(Level.INFO, format("handle:: Data import event payload has been received with event type: %s", eventPayload.getEventType()), debugInfo);

      if (cancelledJobsIdCache.contains(eventPayload.getJobExecutionId())) {
        LOGGER.info("Skip processing of event, topic: '{}', tenantId: '{}', jobExecutionId: '{}' because the job has been cancelled",
          targetRecord.topic(), eventPayload.getTenant(), eventPayload.getJobExecutionId());
        return Future.succeededFuture(targetRecord.key());
      }

      eventPayload.getContext().put(RECORD_ID_HEADER, recordId);
      eventPayload.getContext().put(CHUNK_ID_HEADER, chunkId);
      eventPayload.getContext().put(USER_ID_HEADER, userId);
      populateWithPermissionsHeader(eventPayload, targetRecord);

      OkapiConnectionParams params = RestUtil.retrieveOkapiConnectionParams(eventPayload, vertx);
      String jobProfileSnapshotId = eventPayload.getContext().get(PROFILE_SNAPSHOT_ID_KEY);
      printLogInfo(DEBUG, format("handle:: Using jobProfileSnapshotId: %s", jobProfileSnapshotId), debugInfo);

      profileSnapshotCache.get(jobProfileSnapshotId, params)
        .onFailure(e -> {
          printLogInfo(ERROR, format("handle:: Failed to process record from topic %s", targetRecord.topic()), e, debugInfo);
          sendPayloadWithDiError(eventPayload);
          promise.fail(e);
        })
        .toCompletionStage()
        .thenCompose(snapshotOptional -> {
          printLogInfo(DEBUG, format("handle:: Is snapshot profile with id: '%s' is received: '%s'", jobProfileSnapshotId, snapshotOptional.isPresent()), debugInfo);
          return snapshotOptional.map(profileSnapshot -> {
              printLogInfo(DEBUG, "handle:: Handle event by EventManager", debugInfo);
              return EventManager.handleEvent(eventPayload, profileSnapshot);
            })
            .orElse(CompletableFuture.failedFuture(new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", jobProfileSnapshotId))));
        })
        .whenComplete((processedPayload, throwable) -> {
          if (throwable != null) {
            printLogInfo(ERROR, format("handle:: Failed to process data import event payload from topic %s", targetRecord.topic()), throwable, debugInfo);
            promise.fail(throwable);
          } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
            printLogInfo(ERROR, format("handle:: Failed to process data import event payload from topic %s", targetRecord.topic()), debugInfo);
            promise.fail(format("Failed to process data import event payload from topic '%s' by jobExecutionId: '%s' with recordId: '%s' and chunkId: '%s' ",  targetRecord.topic(), jobExecutionId, recordId, chunkId));
          } else {
            printLogInfo(INFO, format("handle:: Successfully processed data import event payload from topic %s", targetRecord.topic()), debugInfo);
            promise.complete(targetRecord.key());
          }
        });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error("handle:: Failed to process data import kafka record from topic '{}' with recordId: '{}' and chunkId: '{}' ", targetRecord.topic(), recordId, chunkId, e);
      return Future.failedFuture(e);
    }
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

  private void populateWithPermissionsHeader(DataImportEventPayload eventPayload,
                                             KafkaConsumerRecord<String, byte[]> kafkaRecord) {
    String permissions = extractHeaderValue(PERMISSIONS, kafkaRecord.headers());
    if (permissions != null) {
      eventPayload.getContext().put(PERMISSIONS, permissions);
    }
  }

}
