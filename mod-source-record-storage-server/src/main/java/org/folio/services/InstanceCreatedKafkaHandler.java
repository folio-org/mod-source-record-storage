package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.util.Strings;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.util.OkapiConnectionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Component
@Qualifier("InstanceCreatedKafkaHandler")
public class InstanceCreatedKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceCreatedKafkaHandler.class);

  private static final AtomicInteger indexer = new AtomicInteger();

  private Vertx vertx;
  private InstanceEventHandlingService instanceEventHandlingService;


  public InstanceCreatedKafkaHandler(@Autowired InstanceEventHandlingService instanceEventHandlingService,
                                     @Autowired Vertx vertx) {
    this.instanceEventHandlingService = instanceEventHandlingService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = fromKafkaHeaders(kafkaHeaders);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");

    Event event = new JsonObject(record.value()).mapTo(Event.class);
    LOGGER.debug("DataImportEventPayload has been received, starting processing correlationId:" + correlationId + " chunkNumber:" + chunkNumber);

    return instanceEventHandlingService.handleEvent(event.getEventPayload(), okapiConnectionParams)
      .map(v -> {
        LOGGER.debug("DataImportEventPayload processing has been completed correlationId:" + correlationId + " chunkNumber:" + chunkNumber);
        return record.key();
      })
      .onFailure(th -> {
        LOGGER.error("DataImportEventPayload processing has failed with errors correlationId:" + correlationId + " chunkNumber:" + chunkNumber);
      });

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
