package org.folio.consumers;

import static org.folio.dao.util.QMEventTypes.QM_ERROR;
import static org.folio.dao.util.QMEventTypes.QM_SRS_MARC_RECORD_UPDATED;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.services.util.EventHandlingUtil.createProducer;
import static org.folio.services.util.EventHandlingUtil.createProducerRecord;
import static org.folio.services.util.EventHandlingUtil.toOkapiHeaders;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.QMEventTypes;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.services.RecordService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class QuickMarcKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger log = LogManager.getLogger();

  private static final String PARSED_RECORD_DTO_KEY = "PARSED_RECORD_DTO";
  private static final String SNAPSHOT_ID_KEY = "SNAPSHOT_ID";
  private static final String ERROR_KEY = "ERROR";

  private static final AtomicInteger indexer = new AtomicInteger();

  private final Vertx vertx;
  private final RecordService recordService;
  private final KafkaConfig kafkaConfig;

  private final Map<QMEventTypes, KafkaProducer<String, String>> producerMap = new EnumMap<>(QMEventTypes.class);

  @Value("${srs.kafka.QuickMarcKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public QuickMarcKafkaHandler(Vertx vertx, RecordService recordService, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.recordService = recordService;
    this.kafkaConfig = kafkaConfig;

    producerMap.put(QM_SRS_MARC_RECORD_UPDATED, createProducer(QM_SRS_MARC_RECORD_UPDATED.name(), kafkaConfig));
    producerMap.put(QM_ERROR, createProducer(QM_SRS_MARC_RECORD_UPDATED.name(), kafkaConfig));
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    log.trace("handle:: Handling kafka consumerRecord {}", consumerRecord);

    var kafkaHeaders = consumerRecord.headers();
    var okapiHeaders = toOkapiHeaders(kafkaHeaders);

    return getEventPayload(consumerRecord)
      .compose(eventPayload -> {
        String snapshotId = eventPayload.getOrDefault(SNAPSHOT_ID_KEY, UUID.randomUUID().toString());
        var tenantId = okapiHeaders.get(OKAPI_TENANT_HEADER);
        return getRecordDto(eventPayload)
          .compose(recordDto -> recordService.updateSourceRecord(recordDto, snapshotId, okapiHeaders))
          .compose(updatedRecord -> {
            eventPayload.put(updatedRecord.getRecordType().value(), Json.encode(updatedRecord));
            return sendEvent(eventPayload, QM_SRS_MARC_RECORD_UPDATED, tenantId, kafkaHeaders)
              .map(aBoolean -> consumerRecord.key());
          })
          .recover(th -> {
            log.warn("handle:: Failed to handle QM_RECORD_UPDATED event", th);
            eventPayload.put(ERROR_KEY, th.getMessage());
            return sendEvent(eventPayload, QM_ERROR, tenantId, kafkaHeaders)
              .map(aBoolean -> th.getMessage());
          });
      })
      .recover(th -> {
          log.warn("handle:: Failed to handle QM_RECORD_UPDATED event", th);
          return Future.failedFuture(th);
        }
      );
  }

  private Future<Boolean> sendEvent(Object payload, QMEventTypes eventType, String tenantId,
                                    List<KafkaHeader> kafkaHeaders) {
    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
    return sendEventToKafka(tenantId, Json.encode(payload), eventType, kafkaHeaders, kafkaConfig, key);
  }

  private Future<Boolean> sendEventToKafka(String tenantId, String eventPayload, QMEventTypes eventType,
                                           List<KafkaHeader> kafkaHeaders, KafkaConfig kafkaConfig, String key) {
    Promise<Boolean> promise = Promise.promise();
    try {
      var producer = producerMap.get(eventType);
      var record = createProducerRecord(eventPayload, eventType.name(), key, tenantId, kafkaHeaders, kafkaConfig);
      return producer.write(record)
        .map(true)
        .onSuccess(v -> log.info("sendEventToKafka:: Event with type {} was sent to kafka", eventType))
        .onFailure(e -> log.warn("sendEventToKafka:: Failed to sent event {}, cause:", eventType, e));
    } catch (Exception e) {
      log.warn("sendEventToKafka:: Failed to send an event for eventType {}, cause:", eventType, e);
      return Future.failedFuture(e);
    }
  }

  @SuppressWarnings("unchecked")
  private Future<HashMap<String, String>> getEventPayload(KafkaConsumerRecord<String, String> consumerRecord) {
    try {
      var event = Json.decodeValue(consumerRecord.value(), Event.class);
      var eventPayload = Json.decodeValue(event.getEventPayload(), HashMap.class);
      return Future.succeededFuture(eventPayload);
    } catch (Exception e) {
      log.warn("getEventPayload:: Failed to get event payload", e);
      return Future.failedFuture(e);
    }
  }

  private Future<ParsedRecordDto> getRecordDto(HashMap<String, String> eventPayload) {
    String parsedRecordDtoJson = eventPayload.get(PARSED_RECORD_DTO_KEY);
    if (StringUtils.isEmpty(parsedRecordDtoJson)) {
      var error = "sendEventToKafka:: Event payload does not contain required PARSED_RECORD_DTO data";
      log.warn(error);
      return Future.failedFuture(error);
    } else {
      return Future.succeededFuture(Json.decodeValue(parsedRecordDtoJson, ParsedRecordDto.class));
    }
  }
}
