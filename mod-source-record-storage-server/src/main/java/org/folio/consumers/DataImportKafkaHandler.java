package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

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
import org.folio.services.caches.JobProfileSnapshotCache;
import org.folio.services.util.RestUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, byte[]> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String PROFILE_SNAPSHOT_ID_KEY = "JOB_PROFILE_SNAPSHOT_ID";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  static final String USER_ID_HEADER = "userId";

  private Vertx vertx;
  private KafkaConfig kafkaConfig;
  private JobProfileSnapshotCache profileSnapshotCache;

  @Autowired
  public DataImportKafkaHandler(Vertx vertx, JobProfileSnapshotCache profileSnapshotCache, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.profileSnapshotCache = profileSnapshotCache;
    this.kafkaConfig = kafkaConfig;
  }

  // TODO: possible wrong placement of entity logs when cache error
  private void sendPayloadWithDiError2(DataImportEventPayload eventPayload) {
    LOGGER.info("sendPayloadWithDiError:: Sending event with type '{}' to kafka topic '{}' with jobExecutionId: '{}' ",
      DI_ERROR.value(), kafkaConfig.getEnvId(), eventPayload.getJobExecutionId());
    eventPayload.setEventType(DI_ERROR.value());
    try (var eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      eventPublisher.publish(eventPayload);
      var eventType = eventPayload.getEventType();
      LOGGER.warn("publish:: {} send error for event: '{}' by jobExecutionId: '{}' ",
        eventType + "_Producer",
        eventType,
        eventPayload.getJobExecutionId());
    } catch (Exception e) {
      LOGGER.error("Error closing kafka publisher: {}", e.getMessage());
    }
  }

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
    LOGGER.debug("sendEventWithRetry:: Sending event with type '{}' to kafka topic '{}' with jobExecutionId: '{}' and recordId: '{}'",
      DI_ERROR.value(), kafkaConfig.getEnvId(), eventPayload.getJobExecutionId(), recordId);
    try (var eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      eventPublisher.publish(eventPayload)
        .whenComplete((event, throwable) -> {
          if (throwable != null) {
            if (retryCount < maxRetryCount) {
              LOGGER.error("sendPayloadWithDiError:: Error sending event with type '{}' with jobExecutionId: '{}' and recordId: '{}', retry attempt {}: {}",
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
      LOGGER.info("handle:: Data import event payload has been received with event type: '{}' by jobExecutionId: '{}' and recordId: '{}' and chunkId: '{}' and userId: '{}'",
        eventPayload.getEventType(), eventPayload.getJobExecutionId(), recordId, chunkId, userId);

      eventPayload.getContext().put(RECORD_ID_HEADER, recordId);
      eventPayload.getContext().put(CHUNK_ID_HEADER, chunkId);
      eventPayload.getContext().put(USER_ID_HEADER, userId);

      OkapiConnectionParams params = RestUtil.retrieveOkapiConnectionParams(eventPayload, vertx);
      String jobProfileSnapshotId = eventPayload.getContext().get(PROFILE_SNAPSHOT_ID_KEY);
      LOGGER.info("handle:: debug 2: recordId: {}, jobProfileSnapshotId: {}", recordId, jobProfileSnapshotId);
      profileSnapshotCache.get(jobProfileSnapshotId, params)
        .onFailure(e -> {
          LOGGER.error("handle:: debug 2.1: Failed to process data import event payload from topic '{}' by jobExecutionId: '{}' with recordId: '{}' and chunkId: '{}' ",
            targetRecord.topic(), eventPayload.getJobExecutionId(), recordId, chunkId, e);
          sendPayloadWithDiError(eventPayload);
          promise.fail(e);
        })
        .toCompletionStage()
        .thenCompose(snapshotOptional -> {
          LOGGER.debug("handle:: debug 2.2: snapshotOptional.isPresent: {} for jobProfileSnapshotId {}", snapshotOptional.isPresent(), jobProfileSnapshotId);
          return snapshotOptional
            .map(profileSnapshot -> {
                LOGGER.debug("handle:: debug 2.3: EventManager.handleEvent for jobExecutionId: '{}' with recordId: '{}'",
                  eventPayload.getJobExecutionId(), recordId);
                return EventManager.handleEvent(eventPayload, profileSnapshot);
            })
            .orElse(CompletableFuture.failedFuture(new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", jobProfileSnapshotId))));
        })
        .whenComplete((processedPayload, throwable) -> {
          LOGGER.info("handle:: debug 2.5: recordId: {}, chunkId: {}", recordId, chunkId);
          if (throwable != null) {
            LOGGER.error("handle:: debug 3: Failed to process data import event payload from topic '{}' by jobExecutionId: '{}' with recordId: '{}' and chunkId: '{}' ",
              targetRecord.topic(), eventPayload.getJobExecutionId(), recordId, chunkId, throwable);
            promise.fail(throwable);
          } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
            LOGGER.error("handle:: debug 4: Failed to process data import event payload from topic '{}' by jobExecutionId: '{}' with recordId: '{}' and chunkId: '{}' ",
              targetRecord.topic(), eventPayload.getJobExecutionId(), recordId, chunkId);
            promise.fail(format("handle:: Failed to process data import event payload from topic '%s' by jobExecutionId: '%s' with recordId: '%s' and chunkId: '%s' ", targetRecord.topic(),
              eventPayload.getJobExecutionId(), recordId, chunkId));
          } else {
            try {
              LOGGER.info("handle:: debug 5: Successfully processed data import event payload from topic '{}' by jobExecutionId: '{}' with recordId: '{}' and chunkId: '{}' ",
                targetRecord.topic(), eventPayload.getJobExecutionId(), recordId, chunkId);
              promise.complete(targetRecord.key());
            } catch (Exception e) {
              LOGGER.error("handle:: debug 6: Failed to process data import kafka record from topic '{}' by jobExecutionId: '{}' with recordId: '{}' and chunkId: '{}' ",
                targetRecord.topic(), eventPayload.getJobExecutionId(), recordId, chunkId, e);
              promise.fail(e);
            }
          }
        });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error("handle:: Failed to process data import kafka record from topic '{}' with recordId: '{}' and chunkId: '{}' ", targetRecord.topic(), recordId, chunkId, e);
      return Future.failedFuture(e);
    }
  }
}
