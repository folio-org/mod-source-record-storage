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
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
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
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Event event = Json.decodeValue(record.value(), Event.class);
    RecordCollection recordCollection = Json.decodeValue(event.getEventPayload(), RecordCollection.class);

    List<KafkaHeader> kafkaHeaders = record.headers();

    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String tenantId = okapiConnectionParams.getTenantId();
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String key = record.key();

    int chunkNumber = chunkCounter.incrementAndGet();

    try {
      LOGGER.debug("RecordCollection has been received, correlationId: {}, starting processing... chunkNumber {}-{}", correlationId, chunkNumber, key);
      return recordService.saveRecords(recordCollection, tenantId)
        .compose(recordsBatchResponse -> sendBackRecordsBatchResponse(recordsBatchResponse, kafkaHeaders, tenantId, correlationId, chunkNumber));
    } catch (Exception e) {
      LOGGER.error("RecordCollection processing has failed with errors... correlationId: {}, chunkNumber {}-{}", correlationId, chunkNumber, key, e);
      return Future.failedFuture(e);
    }
  }

  private Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, String correlationId, int chunkNumber) {
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

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    record.addHeaders(kafkaHeaders);

    Promise<String> writePromise = Promise.promise();

    String producerName = DI_PARSED_RECORDS_CHUNK_SAVED + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        LOGGER.debug("RecordCollection processing has been completed with response sent... correlationId {}, chunkNumber {}-{}", correlationId, chunkNumber, record.key());
        writePromise.complete(record.key());
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
      .stream().peek(record -> {
        if (record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
          String content = ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord());
          record.getParsedRecord().withContent(content);
        }
      }).collect(Collectors.toList()));
  }
}
