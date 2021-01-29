package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED;

@Component
@Qualifier("ParsedMarcChunksKafkaHandler")
public class ParsedMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParsedMarcChunksKafkaHandler.class);

  private static final AtomicInteger chunkCounter = new AtomicInteger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private RecordService recordService;
  private Vertx vertx;
  private KafkaConfig kafkaConfig;

  @Value("${srs.kafka.ParsedMarcChunksKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public ParsedMarcChunksKafkaHandler(@Autowired RecordService recordService,
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
      RecordCollection recordCollection = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(RecordCollection.class);

      List<KafkaHeader> kafkaHeaders = record.headers();

      String tenantId = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx).getTenantId();
      String key = record.key();

      int chunkNumber = chunkCounter.incrementAndGet();
      LOGGER.debug("RecordCollection has been received, starting processing... chunkNumber {}-{}", chunkNumber, key);
      return recordService.saveRecords(recordCollection, tenantId)
        .compose(recordsBatchResponse -> sendBackRecordsBatchResponse(recordsBatchResponse, kafkaHeaders, tenantId, chunkNumber),
          th -> {
            LOGGER.error("RecordCollection processing has failed with errors... chunkNumber {}-{}", th, chunkNumber, key);
            return Future.failedFuture(th);
          });
    } catch (IOException e) {
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

  private Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, int chunkNumber) {
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED.value())
        .withEventPayload(ZIPArchiver.zip(Json.encode(recordsBatchResponse)))
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
      tenantId, DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED.value());

    LOGGER.info("Sending event {}", Json.encode(event));

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    record.addHeaders(kafkaHeaders);

    Promise<String> writePromise = Promise.promise();

    String producerName = DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        LOGGER.debug("RecordCollection processing has been completed with response sent... chunkNumber {}-{}",chunkNumber, record.key());
        writePromise.complete(record.key());
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error:", cause, producerName);
        writePromise.fail(cause);
      }
    });
    return writePromise.future();
  }

}
