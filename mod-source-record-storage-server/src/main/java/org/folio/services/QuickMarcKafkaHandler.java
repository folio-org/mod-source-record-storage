package org.folio.services;

import static org.folio.dao.util.QMEventTypes.QM_ERROR;
import static org.folio.dao.util.QMEventTypes.QM_SRS_MARC_RECORD_UPDATED;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.folio.dao.util.QMEventTypes;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.util.OkapiConnectionParams;

@Component
public class QuickMarcKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger log = LogManager.getLogger();

  private static final String PARSED_RECORD_DTO_KEY = "PARSED_RECORD_DTO";
  private static final String SNAPSHOT_ID_KEY = "SNAPSHOT_ID";
  private static final String ERROR_KEY = "ERROR";
  private static final String MARC_KEY = "MARC";

  private static final AtomicInteger indexer = new AtomicInteger();

  private final Vertx vertx;
  private final RecordService recordService;
  private final KafkaConfig kafkaConfig;

  @Value("${srs.kafka.QuickMarcKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public QuickMarcKafkaHandler(Vertx vertx, RecordService recordService, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.recordService = recordService;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    var kafkaHeaders = record.headers();
    var params = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);

    return getEventPayload(record)
      .compose(eventPayload -> {
        String snapshotId = eventPayload.getOrDefault(SNAPSHOT_ID_KEY, UUID.randomUUID().toString());
        return getRecordDto(eventPayload)
          .compose(recordDto -> recordService.updateSourceRecord(recordDto, snapshotId, params.getTenantId()))
          .compose(updatedRecord -> {
            eventPayload.put(MARC_KEY, Json.encode(updatedRecord));
            return sendEvent(eventPayload, QM_SRS_MARC_RECORD_UPDATED, params.getTenantId(), kafkaHeaders)
              .map(aBoolean -> record.key());
          })
          .recover(th -> {
            eventPayload.put(ERROR_KEY, th.getMessage());
            return sendEvent(eventPayload, QM_ERROR, params.getTenantId(), kafkaHeaders)
              .map(aBoolean -> th.getMessage());
          });
      })
      .recover(th -> {
          log.error("Failed to handle QM_RECORD_UPDATED event", th);
          return Future.failedFuture(th);
        }
      );
  }

  @SuppressWarnings("unchecked")
  private Future<HashMap<String, String>> getEventPayload(KafkaConsumerRecord<String, String> record) {
    try {
      var event = Json.decodeValue(record.value(), Event.class);
      var eventPayload = Json.decodeValue(ZIPArchiver.unzip(event.getEventPayload()), HashMap.class);
      return Future.succeededFuture(eventPayload);
    } catch (IOException e) {
      return Future.failedFuture(e);
    }
  }

  private Future<ParsedRecordDto> getRecordDto(HashMap<String, String> eventPayload) {
    String parsedRecordDtoJson = eventPayload.get(PARSED_RECORD_DTO_KEY);
    if (StringUtils.isEmpty(parsedRecordDtoJson)) {
      var error = "Event payload does not contain required PARSED_RECORD_DTO data";
      log.error(error);
      return Future.failedFuture(error);
    } else {
      return Future.succeededFuture(Json.decodeValue(parsedRecordDtoJson, ParsedRecordDto.class));
    }
  }

  private Future<Boolean> sendEvent(Object payload, QMEventTypes eventType, String tenantId,
                                    List<KafkaHeader> kafkaHeaders) {
    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
    return sendEventToKafka(tenantId, Json.encode(payload), eventType.name(), kafkaHeaders, kafkaConfig, key);
  }
}
