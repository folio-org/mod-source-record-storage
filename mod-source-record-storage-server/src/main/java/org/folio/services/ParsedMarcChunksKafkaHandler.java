package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.util.Strings;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
@Qualifier("ParsedMarcChunksKafkaHandler")
public class ParsedMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParsedMarcChunksKafkaHandler.class);

  //TODO: make it an ENUM value
  public static final String DI_PARSED_MARCS_CHUNK_STORED_EVENT_TYPE = "DI_PARSED_MARCS_CHUNK_STORED";

  private static final AtomicInteger chunkCounter = new AtomicInteger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private RecordService recordService;
  private Vertx vertx;
  private KafkaConfig kafkaConfig;

  //TODO: make it configurable
  private int maxDistributionNum = 100;


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

      String tenantId = fromKafkaHeaders(kafkaHeaders).getTenantId();
      String key = record.key();

      int chunkNumber = chunkCounter.incrementAndGet();
      LOGGER.debug("RecordCollection has been received, starting processing... chunkNumber " + chunkNumber + " - " + key);
      return recordService.saveRecords(recordCollection, tenantId)
        .compose(recordsBatchResponse -> sendBackRecordsBatchResponse(recordsBatchResponse, kafkaHeaders, tenantId, chunkNumber),
          th -> {
            th.printStackTrace();
            LOGGER.error("RecordCollection processing has failed with errors... chunkNumber " + chunkNumber + " - " + key, th);
            return Future.failedFuture(th);
          });
    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

  private Future<String> sendBackRecordsBatchResponse(RecordsBatchResponse recordsBatchResponse, List<KafkaHeader> kafkaHeaders, String tenantId, int chunkNumber) {
    //TODO: this could be some utility method
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(DI_PARSED_MARCS_CHUNK_STORED_EVENT_TYPE)
        .withEventPayload(ZIPArchiver.zip(Json.encode(recordsBatchResponse)))
        .withEventMetadata(new EventMetadata()
          .withTenantId(tenantId)
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));
    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Future.failedFuture(e);
    }

    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, DI_PARSED_MARCS_CHUNK_STORED_EVENT_TYPE);

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    record.addHeaders(kafkaHeaders);

    Promise<String> writePromise = Promise.promise();

    String producerName = DI_PARSED_MARCS_CHUNK_STORED_EVENT_TYPE + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        LOGGER.debug("RecordCollection processing has been completed with response sent... chunkNumber " + chunkNumber + " - " + record.key());
        writePromise.complete(record.key());
      } else {
        Throwable cause = war.cause();
        LOGGER.error(producerName + " write error:", cause);
        writePromise.fail(cause);
      }
    });
    return writePromise.future();
  }


  //TODO: utility method must be moved out from here
  private OkapiConnectionParams fromKafkaHeaders(List<KafkaHeader> headers) {
    Map<String, String> okapiHeaders = headers
      .stream()
      .collect(Collectors.groupingBy(KafkaHeader::key,
        Collectors.reducing(Strings.EMPTY,
          header -> {
            Buffer value = header.value();
            return Objects.isNull(value) ? "" : value.toString();
          },
          (a, b) -> Strings.isNotBlank(a) ? a : b)));

    return new OkapiConnectionParams(okapiHeaders, vertx);
  }

}
