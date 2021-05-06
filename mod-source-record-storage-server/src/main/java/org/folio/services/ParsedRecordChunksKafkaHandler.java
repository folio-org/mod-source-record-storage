package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
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
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.util.EventHandlingUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;

@Component
@Qualifier("ParsedRecordChunksKafkaHandler")
public class ParsedRecordChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String ERROR_KEY = "ERROR";
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
    Event event = new JsonObject(record.value()).mapTo(Event.class);

    try {
      RecordCollection recordCollection = Json.decodeValue(ZIPArchiver.unzip(event.getEventPayload()), RecordCollection.class);

      List<KafkaHeader> kafkaHeaders = record.headers();

      OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
      String tenantId = okapiConnectionParams.getTenantId();
      String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
      String jobExecutionId = okapiConnectionParams.getHeaders().get(JOB_EXECUTION_ID_HEADER);
      String key = record.key();

      int chunkNumber = chunkCounter.incrementAndGet();
      LOGGER.debug("RecordCollection has been received, correlationId: {}, starting processing... chunkNumber {}-{}", correlationId, chunkNumber, key);
      return recordService.saveRecords(recordCollection, tenantId)
        .compose(recordsBatchResponse -> sendBackRecordsBatchResponse(recordsBatchResponse, kafkaHeaders, tenantId, correlationId, chunkNumber),
          th -> {
            LOGGER.error("RecordCollection processing has failed with errors... correlationId: {}, chunkNumber {}-{}", correlationId, chunkNumber, key, th);
            return sendErrorRecordsSavingEvents(recordCollection, th.getMessage(), kafkaHeaders, jobExecutionId, tenantId)
              .compose(v -> Future.failedFuture(th));
          });
    } catch (Exception e) {
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

  private Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, String correlationId, int chunkNumber) {
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(DI_PARSED_RECORDS_CHUNK_SAVED.value())
        .withEventPayload(ZIPArchiver.zip(Json.encode(normalize(recordsBatchResponse))))
        .withEventMetadata(new EventMetadata()
          .withTenantId(tenantId)
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));
    } catch (IOException e) {
      LOGGER.error("Error constructing event payload", e);
      return Future.failedFuture(e);
    }

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
      .stream().map(record -> {
        String content = ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord());
        return record.withParsedRecord(record.getParsedRecord().withContent(content));
      }).collect(Collectors.toList()));
  }

  private Future<Void> sendErrorRecordsSavingEvents(RecordCollection recordCollection, String message, List<KafkaHeader> kafkaHeaders, String jobExecutionId, String tenantId) {
    List<Future<Boolean>> sendingFutures = new ArrayList<>();
    for (Record record : recordCollection.getRecords()) {
      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_ERROR.value())
        .withJobExecutionId(jobExecutionId)
        .withEventsChain(List.of(DI_SRS_MARC_BIB_RECORD_CREATED.value()))
        .withContext(new HashMap<>(){{
          put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
          put(ERROR_KEY, message);
        }});

      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
      sendingFutures.add(EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(dataImportEventPayload), DI_ERROR.value(), kafkaHeaders, kafkaConfig, key));
    }

    Promise<Void> promise = Promise.promise();
    GenericCompositeFuture.join(sendingFutures)
      .onSuccess(v -> promise.complete())
      .onFailure(th -> {
        LOGGER.warn("Failed to send records sending error events" , th);
        promise.fail(th);
      });

    return promise.future();
  }

}
