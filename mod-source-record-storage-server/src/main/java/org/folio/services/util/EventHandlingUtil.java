package org.folio.services.util;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

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
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.tools.utils.ModuleName;

public final class EventHandlingUtil {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";


  private EventHandlingUtil() { }

  /**
   * Prepares and sends event with payload to kafka
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
    KafkaProducerRecord<String, String> record;
    try {
      record = createProducerRecord(eventPayload, eventType, key, tenantId, kafkaHeaders, kafkaConfig, false);
    } catch (IOException e) {
      LOGGER.error("Failed to construct an event for eventType {}", eventType, e);
      return Future.failedFuture(e);
    }

    Promise<Boolean> promise = Promise.promise();

    String recordId = extractRecordId(kafkaHeaders);
    String chunkId = extractChunkId(kafkaHeaders);
    String producerName = eventType + "_Producer";
    var producer = createProducer(eventType, kafkaConfig);

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        LOGGER.info("Event with type {} and recordId {}  with chunkId: {} was sent to kafka", eventType, recordId, chunkId);
        promise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error for event {} with recordId {} with chunkId: {}, cause:",  producerName, eventType, recordId, chunkId, cause);
        promise.fail(cause);
      }
    });
    return promise.future();
  }

  public static KafkaProducerRecord<String, String> createProducerRecord(String eventPayload, String eventType,
                                                                         String key, String tenantId,
                                                                         List<KafkaHeader> kafkaHeaders,
                                                                         KafkaConfig kafkaConfig,
                                                                         boolean zippedPayload) throws IOException {
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(zippedPayload ? ZIPArchiver.zip(eventPayload) : eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));

    String topicName = createTopicName(eventType, tenantId, kafkaConfig);

    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeaders);
    return record;
  }

  public static String constructModuleName() {
    return PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(ModuleName.getModuleName(),
      ModuleName.getModuleVersion());
  }
  public static String createTopicName(String eventType, String tenantId, KafkaConfig kafkaConfig) {
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);
  }

  public static KafkaProducer<String, String> createProducer(String eventType, KafkaConfig kafkaConfig) {
    String producerName = eventType + "_Producer";
    return KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());
  }

  private static String extractRecordId(List<KafkaHeader> kafkaHeaders) {
    return kafkaHeaders.stream()
      .filter(header -> header.key().equals(RECORD_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }

  private static String extractChunkId(List<KafkaHeader> kafkaHeaders) {
    return kafkaHeaders.stream()
      .filter(header -> header.key().equals("chunkId"))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
