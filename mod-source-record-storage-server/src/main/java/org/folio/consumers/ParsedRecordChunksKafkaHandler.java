package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.RecordService;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.services.util.EventHandlingUtil.constructModuleName;
import static org.folio.services.util.EventHandlingUtil.toOkapiHeaders;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;

@Component
public class ParsedRecordChunksKafkaHandler implements AsyncRecordHandler<String, byte[]> {
  private static final Logger LOGGER = LogManager.getLogger();

  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final String CHUNK_ID_HEADER = "chunkId";
  private static final String USER_ID_HEADER = "userId";
  private static final AtomicInteger chunkCounter = new AtomicInteger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private RecordService recordService;
  private Vertx vertx;
  private KafkaConfig kafkaConfig;
  private final SimpleKafkaProducerManager producerManager;
  private final CancelledJobsIdsCache cancelledJobsIdCache;

  // TODO: refactor srs.kafka.ParsedRecordChunksKafkaHandler
  @Value("${srs.kafka.ParsedRecordChunksKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  @Autowired
  public ParsedRecordChunksKafkaHandler(RecordService recordService,
                                        CancelledJobsIdsCache cancelledJobsIdsCache,
                                        Vertx vertx,
                                        KafkaConfig kafkaConfig) {
    this.recordService = recordService;
    this.cancelledJobsIdCache = cancelledJobsIdsCache;
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
    producerManager = new SimpleKafkaProducerManager(vertx, kafkaConfig);
  }

  @Override
  @SuppressWarnings("squid:S2629")
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> targetRecord) {
    LOGGER.trace("handle:: Handling kafka record: {}", targetRecord);
    String jobExecutionId = extractHeaderValue(JOB_EXECUTION_ID_HEADER, targetRecord.headers());
    String chunkId = extractHeaderValue(CHUNK_ID_HEADER, targetRecord.headers());
    String userId = extractHeaderValue(USER_ID_HEADER, targetRecord.headers());
    int chunkNumber = chunkCounter.incrementAndGet();
    String key = targetRecord.key();

    try {
      if (cancelledJobsIdCache.contains(jobExecutionId)) {
        LOGGER.info("handle:: Skipping processing of event, topic: '{}', jobExecutionId: '{}' because the job has been cancelled",
          targetRecord.topic(), jobExecutionId);
        return Future.succeededFuture(targetRecord.key());
      }

      Event event = DatabindCodec.mapper().readValue(targetRecord.value(), Event.class);
      RecordCollection recordCollection = Json.decodeValue(event.getEventPayload(), RecordCollection.class);

      List<KafkaHeader> kafkaHeaders = targetRecord.headers();
      OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
      String tenantId = okapiConnectionParams.getTenantId();

      LOGGER.debug("handle:: RecordCollection has been received with event: '{}', jobExecutionId '{}', chunkId: '{}', starting processing... chunkNumber '{}'-'{}'",
        event.getEventType(), jobExecutionId, chunkId, chunkNumber, key);
      setUserMetadata(recordCollection, userId);
      return recordService.saveRecords(recordCollection, toOkapiHeaders(kafkaHeaders))
        .compose(recordsBatchResponse -> sendBackRecordsBatchResponse(recordsBatchResponse, kafkaHeaders, tenantId, chunkNumber, event.getEventType(), targetRecord));
    } catch (Exception e) {
      LOGGER.warn("handle:: RecordCollection processing has failed with errors jobExecutionId '{}', chunkId: '{}', chunkNumber '{}'-'{}'",
        jobExecutionId, chunkId, chunkNumber, key);
      return Future.failedFuture(e);
    }
  }

  private Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, int chunkNumber, String eventType, KafkaConsumerRecord<String, byte[]> commonRecord) {
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

    var targetRecord =
      new KafkaProducerRecordBuilder<String, Object>(tenantId)
        .key(key)
        .value(event)
        .topic(topicName)
        .build();

    targetRecord.addHeaders(kafkaHeaders);

    Promise<String> writePromise = Promise.promise();

    String producerName = DI_PARSED_RECORDS_CHUNK_SAVED + "_Producer";
    KafkaProducer<String, String> producer = producerManager.createShared(DI_PARSED_RECORDS_CHUNK_SAVED.value());

    producer.send(targetRecord)
      .<Void>mapEmpty()
      .eventually((Supplier<Future<Void>>) producer::close)
      .onSuccess(res -> {
        String chunkId = extractHeaderValue(CHUNK_ID_HEADER, commonRecord.headers());
        LOGGER.debug("sendBackRecordsBatchResponse:: RecordCollection processing has been completed with response sent... event: '{}', chunkId: '{}', chunkNumber '{}'-'{}'",
          eventType, chunkId, chunkNumber, targetRecord.key());
        writePromise.complete(targetRecord.key());
      })
      .onFailure(err -> {
        Throwable cause = err.getCause();
        LOGGER.warn("sendBackRecordsBatchResponse:: {} write error {}", producerName, cause);
        writePromise.fail(cause);
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
      }).toList());
  }

  private void setUserMetadata(RecordCollection recordCollection, String userId) {
    recordCollection.getRecords()
      .forEach(mRecord -> {
        if (mRecord.getMetadata() != null) {
          mRecord.getMetadata().setUpdatedByUserId(userId);
        } else {
          mRecord.withMetadata(new Metadata()
            .withCreatedByUserId(userId)
            .withUpdatedByUserId(userId));
        }
      });
  }
}
