package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.DataImportEventPayload;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.RecordService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.services.util.EventHandlingUtil.constructModuleName;

@Component
public class ParsedRecordChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String USER_ID_HEADER = "userId";
  private static final AtomicInteger chunkCounter = new AtomicInteger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private RecordService recordService;
  private Vertx vertx;
  private KafkaConfig kafkaConfig;

  // TODO: refactor srs.kafka.ParsedRecordChunksKafkaHandler
  @Value("${srs.kafka.ParsedRecordChunksKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public ParsedRecordChunksKafkaHandler(@Autowired RecordService recordService,
                                        @Autowired Vertx vertx,
                                        @Autowired KafkaConfig kafkaConfig) {
    this.recordService = recordService;
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> targetRecord) {
    Event event = Json.decodeValue(targetRecord.value(), Event.class);
    RecordCollection recordCollection = Json.decodeValue(event.getEventPayload(), RecordCollection.class);

    List<KafkaHeader> kafkaHeaders = targetRecord.headers();

    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String tenantId = okapiConnectionParams.getTenantId();
    String recordId = extractValueFromHeaders(targetRecord.headers(), RECORD_ID_HEADER);
    String chunkId = extractValueFromHeaders(targetRecord.headers(), CHUNK_ID_HEADER);
    String userId = extractValueFromHeaders(targetRecord.headers(), USER_ID_HEADER);
    String key = targetRecord.key();

    int chunkNumber = chunkCounter.incrementAndGet();
    DataImportEventPayload eventPayload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);

    try {
      LOGGER.debug("RecordCollection has been received with event: '{}', chunkId: '{}', starting processing... chunkNumber '{}'-'{}' with recordId: '{}'' ",
        eventPayload.getEventType(), chunkId, chunkNumber, key, recordId);
      setCreatedBy(recordCollection, userId);
      return recordService.saveRecords(recordCollection, tenantId)
        .compose(recordsBatchResponse -> sendBackRecordsBatchResponse(recordsBatchResponse, kafkaHeaders, tenantId, chunkNumber, eventPayload.getEventType(), targetRecord));
    } catch (Exception e) {
      LOGGER.error("RecordCollection processing has failed with errors with event: '{}', chunkId: '{}', chunkNumber '{}'-'{}' with recordId: '{}' ",
        eventPayload.getEventType(), chunkId, chunkNumber, key, recordId);
      return Future.failedFuture(e);
    }
  }

  private Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, int chunkNumber, String eventType, KafkaConsumerRecord<String, String> commonRecord) {
    Event event;
    event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(DI_PARSED_RECORDS_CHUNK_SAVED.value())
      .withEventPayload(Json.encode(normalize(recordsBatchResponse)))
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));

    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, DI_PARSED_RECORDS_CHUNK_SAVED.value());

    KafkaProducerRecord<String, String> targetRecord =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    targetRecord.addHeaders(kafkaHeaders);

    Promise<String> writePromise = Promise.promise();

    String producerName = DI_PARSED_RECORDS_CHUNK_SAVED + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(targetRecord, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        String recordId = extractValueFromHeaders(commonRecord.headers(), RECORD_ID_HEADER);
        String chunkId = extractValueFromHeaders(commonRecord.headers(), CHUNK_ID_HEADER);
        LOGGER.debug("RecordCollection processing has been completed with response sent... event: '{}', chunkId: '{}', chunkNumber '{}'-'{}' with recordId: '{}'",
          eventType, chunkId, chunkNumber, targetRecord.key(), recordId);
        writePromise.complete(targetRecord.key());
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error {}", producerName, cause);
        writePromise.fail(cause);
      }
    });
    return writePromise.future();
  }

  private RecordsBatchResponse normalize(RecordsBatchResponse recordsBatchResponse) {
    return recordsBatchResponse.withRecords(recordsBatchResponse.getRecords()
      .stream().peek(targetRecord -> {
        if (targetRecord.getParsedRecord() != null && targetRecord.getParsedRecord().getContent() != null) {
          String content = ParsedRecordDaoUtil.normalizeContent(targetRecord.getParsedRecord());
          targetRecord.getParsedRecord().withContent(content);
        }
      }).collect(Collectors.toList()));
  }

  private void setCreatedBy(RecordCollection recordCollection, String userId) {
    recordCollection.getRecords()
      .forEach(record -> {
        if (record.getMetadata() != null) {
          record.getMetadata().setCreatedByUserId(userId);
        } else {
          record.withMetadata(new Metadata().withCreatedByUserId(userId));
        }
      });
  }

  private String extractValueFromHeaders(List<KafkaHeader> headers, String key) {
    return headers.stream()
      .filter(header -> header.key().equals(key))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
