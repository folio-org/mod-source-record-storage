package org.folio.consumers;

import static org.folio.dao.util.QMEventTypes.QM_ERROR;
import static org.folio.dao.util.QMEventTypes.QM_SRS_MARC_RECORD_UPDATED;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.services.util.EventHandlingUtil.createProducer;
import static org.folio.services.util.EventHandlingUtil.createProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.services.RecordService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.folio.dao.util.QMEventTypes;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.util.OkapiConnectionParams;

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

  private final Map<QMEventTypes, KafkaProducer<String, String>> producerMap = new HashMap<>();

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
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    log.trace("handle:: Handling kafka record {}", record);
    var event = Json.decodeValue(record.value(), Event.class);

    var kafkaHeaders = record.headers();
    var params = new OkapiConnectionParams(kafkaHeadersToMap(kafkaHeaders), vertx);

    return getEventPayload(event)
      .compose(eventPayload -> {
        String snapshotId = eventPayload.getOrDefault(SNAPSHOT_ID_KEY, UUID.randomUUID().toString());
        return getRecordDto(eventPayload)
          .compose(recordDto -> recordService.updateSourceRecord(recordDto, snapshotId, params.getTenantId()))
          .compose(updatedRecord -> {
            eventPayload.put(updatedRecord.getRecordType().value(), Json.encode(updatedRecord));
            return sendEvent(eventPayload, QM_SRS_MARC_RECORD_UPDATED, params.getTenantId(), kafkaHeaders)
              .map(aBoolean -> record.key());
          })
          .recover(th -> {
            log.warn("handle:: Failed to handle QM_RECORD_UPDATED event", th);
            eventPayload.put(ERROR_KEY, th.getMessage());
            return sendEvent(eventPayload, QM_ERROR, params.getTenantId(), kafkaHeaders)
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
      producer.write(record, war -> {
        if (war.succeeded()) {
          log.info("sendEventToKafka:: Event with type {} was sent to kafka", eventType);
          promise.complete(true);
        } else {
          Throwable cause = war.cause();
          log.warn("sendEventToKafka:: Failed to sent event {}, cause: {}", eventType, cause);
          promise.fail(cause);
        }
      });
    } catch (Exception e) {
      log.warn("sendEventToKafka:: Failed to send an event for eventType {}, cause {}", eventType, e);
      return Future.failedFuture(e);
    }
    return promise.future();
  }

  @SuppressWarnings("unchecked")
  private Future<HashMap<String, String>> getEventPayload(Event event) {
    try {
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
