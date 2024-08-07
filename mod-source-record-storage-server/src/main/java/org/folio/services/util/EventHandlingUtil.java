package org.folio.services.util;

import static java.util.Arrays.stream;
import static java.util.Objects.nonNull;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.domainevent.RecordDomainEventPublisher.RECORD_DOMAIN_EVENT_TOPIC;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.services.domainevent.SourceRecordDomainEventType;

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
    KafkaProducerRecord<String, String> producerRecord =
      createProducerRecord(eventPayload, eventType, key, tenantId, kafkaHeaders, kafkaConfig);

    Promise<Boolean> promise = Promise.promise();

    String recordId = extractRecordId(kafkaHeaders);
    String chunkId = extractChunkId(kafkaHeaders);
    String producerName = eventType + "_Producer";
    var producer = createProducer(eventType, kafkaConfig);

    producer.send(producerRecord)
      .<Void>mapEmpty()
      .eventually(x -> producer.close())
      .onSuccess(res -> {
        LOGGER.info("sendEventToKafka:: Event with type {} and recordId {}  with chunkId: {} was sent to kafka", eventType, recordId, chunkId);
        promise.complete(true);
      })
      .onFailure(err -> {
        Throwable cause = err.getCause();
        LOGGER.warn("sendEventToKafka:: {} write error for event {} with recordId {} with chunkId: {}, cause:", producerName, eventType, recordId, chunkId, cause);
        promise.fail(cause);
      });
    return promise.future();
  }

  public static KafkaProducerRecord<String, String> createProducerRecord(String eventPayload, String eventType,
                                                                         String key, String tenantId,
                                                                         List<KafkaHeader> kafkaHeaders,
                                                                         KafkaConfig kafkaConfig) {
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));

    String topicName = createTopicName(eventType, tenantId, kafkaConfig);
    var producerRecord = new KafkaProducerRecordBuilder<String, Object>(tenantId)
      .key(key)
      .value(event)
      .topic(topicName)
      .build();

    producerRecord.addHeaders(kafkaHeaders);
    return producerRecord;
  }

  public static String constructModuleName() {
    return PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(ModuleName.getModuleName(),
      ModuleName.getModuleVersion());
  }

  public static String createTopicName(String eventType, String tenantId, KafkaConfig kafkaConfig) {
    if (stream(SourceRecordDomainEventType.values()).anyMatch(et -> et.name().equals(eventType))) {
      return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), tenantId, RECORD_DOMAIN_EVENT_TOPIC);
    }
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);
  }

  public static String createSubscriptionPattern(String env, String eventType) {
    return String.join("\\.", env, "\\w{1,}", eventType);
  }

  public static KafkaProducer<String, String> createProducer(String eventType, KafkaConfig kafkaConfig) {
    return new SimpleKafkaProducerManager(Vertx.currentContext().owner(), kafkaConfig).createShared(eventType);
  }

  public static Map<String, String> toOkapiHeaders(DataImportEventPayload eventPayload) {
    var okapiHeaders = new HashMap<String, String>();
    okapiHeaders.put(OKAPI_URL_HEADER, eventPayload.getOkapiUrl());
    okapiHeaders.put(OKAPI_TENANT_HEADER, eventPayload.getTenant());
    okapiHeaders.put(OKAPI_TOKEN_HEADER, eventPayload.getToken());
    return okapiHeaders;
  }

  public static Map<String, String> toOkapiHeaders(List<KafkaHeader> kafkaHeaders) {
    return toOkapiHeaders(kafkaHeaders, null);
  }

  public static Map<String, String> toOkapiHeaders(List<KafkaHeader> kafkaHeaders, String eventTenantId) {
    var okapiHeaders = new HashMap<String, String>();
    okapiHeaders.put(OKAPI_URL_HEADER, extractHeaderValue(OKAPI_URL_HEADER, kafkaHeaders));
    okapiHeaders.put(OKAPI_TENANT_HEADER, nonNull(eventTenantId) ? eventTenantId : extractHeaderValue(OKAPI_TENANT_HEADER, kafkaHeaders));
    okapiHeaders.put(OKAPI_TOKEN_HEADER, extractHeaderValue(OKAPI_TOKEN_HEADER, kafkaHeaders));
    return okapiHeaders;
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
      .filter(header -> header.key().equals(CHUNK_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
