package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.util.pubsub.PubSubClientUtils;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public final class EventHandlingUtil {

  private EventHandlingUtil() { }

  private static final Logger LOGGER = LogManager.getLogger();

  /**
   * Prepares and sends event with zipped payload to kafka
   *
   * @param tenantId     tenant id
   * @param eventPayload eventPayload in String representation
   * @param eventType    eventType
   * @param kafkaHeaders kafka headers
   * @param kafkaConfig  kafka config
   * @return completed future with true if event was sent successfully
   */
  public static Future<Boolean> sendEventToKafka(String tenantId, String eventPayload, String eventType,
                                                 List<KafkaHeader> kafkaHeaders, KafkaConfig kafkaConfig, String key) {
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(tenantId)
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));
    } catch (IOException e) {
      LOGGER.error("Failed to construct an event for eventType {}", eventType, e);
      return Future.failedFuture(e);
    }

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);

    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeaders);

    Promise<Boolean> promise = Promise.promise();

    String correlationId = extractCorrelationId(kafkaHeaders);
    String producerName = eventType + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        LOGGER.info("Event with type {} and correlationId {} was sent to kafka", eventType, correlationId);
        promise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error for event {} with correlationId {}, cause:",  producerName, eventType, correlationId, cause);
        promise.fail(cause);
      }
    });
    return promise.future();
  }

  private static String extractCorrelationId(List<KafkaHeader> kafkaHeaders) {
    return kafkaHeaders.stream()
      .filter(header -> header.key().equals("correlationId"))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
