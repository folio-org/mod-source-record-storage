package org.folio.consumers;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.dataimport.util.DataImportHeaders.RECORD_ID;

/**
 * Publishes DI_ERROR events to Kafka with a fixed-delay retry.
 */
@Component
public class DiErrorEventSender {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final int MAX_RETRY_COUNT = 3;
  private static final long RETRY_DELAY_MS = 1000;

  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;

  @Autowired
  public DiErrorEventSender(Vertx vertx, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  /**
   * Switches the payload event type to DI_ERROR and publishes it asynchronously (fire and forget).
   */
  public void send(DataImportEventPayload eventPayload) {
    eventPayload.setEventType(DI_ERROR.value());
    vertx.setTimer(RETRY_DELAY_MS, timerId -> publish(eventPayload, 0));
  }

  private void publish(DataImportEventPayload eventPayload, int retryCount) {
    String jobExecutionId = eventPayload.getJobExecutionId();
    String recordId = eventPayload.getContext().get(RECORD_ID);
    LOGGER.info("publish:: Sending event with type '{}' to kafka topic '{}' with jobExecutionId: '{}' and recordId: '{}'",
      DI_ERROR.value(), kafkaConfig.getEnvId(), jobExecutionId, recordId);
    try (var eventPublisher = new KafkaEventPublisher(kafkaConfig, vertx, 100)) {
      eventPublisher.publish(eventPayload)
        .whenComplete((event, throwable) -> handlePublishResult(eventPayload, retryCount, throwable));
    } catch (Exception e) {
      LOGGER.error("publish:: Error creating Kafka publisher: {}", e.getMessage());
    }
  }

  private void handlePublishResult(DataImportEventPayload eventPayload, int retryCount, Throwable throwable) {
    String jobExecutionId = eventPayload.getJobExecutionId();
    String recordId = eventPayload.getContext().get(RECORD_ID);
    if (throwable == null) {
      LOGGER.info("handlePublishResult:: Successfully sent event with type '{}' with jobExecutionId: '{}' and recordId: '{}'",
        DI_ERROR.value(), jobExecutionId, recordId);
    } else if (retryCount < MAX_RETRY_COUNT) {
      LOGGER.info("handlePublishResult:: Error sending event with type '{}' with jobExecutionId: '{}' and recordId: '{}', retry attempt {}: {}",
        DI_ERROR.value(), jobExecutionId, recordId, retryCount + 1, rootMessage(throwable));
      vertx.setTimer(RETRY_DELAY_MS, timerId -> publish(eventPayload, retryCount + 1));
    } else {
      LOGGER.error("handlePublishResult:: Failed to send event with type '{}' with jobExecutionId: '{}' and recordId: '{}' after {} attempts: {}",
        DI_ERROR.value(), jobExecutionId, recordId, MAX_RETRY_COUNT, rootMessage(throwable));
    }
  }

  private static String rootMessage(Throwable throwable) {
    return throwable.getCause() != null ? throwable.getCause().getMessage() : throwable.getMessage();
  }
}
