package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.DataImportEventPayload;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;

import static org.folio.DataImportEventTypes.DI_ERROR;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataImportKafkaHandler.class);

  private Vertx vertx;

  public DataImportKafkaHandler(@Autowired Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      Event event = ObjectMapperTool.getMapper().readValue(record.value(), Event.class);
      DataImportEventPayload eventPayload = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(DataImportEventPayload.class);
      LOGGER.debug("Data import event payload has been received with event type: {}", eventPayload.getEventType());

      EventManager.handleEvent(eventPayload).whenComplete((processedPayload, throwable) -> {
        if (throwable != null) {
          promise.fail(throwable);
        } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
          promise.fail("Failed to process data import event payload");
        } else {
          promise.complete(record.key());
        }
      });
      return promise.future();
    } catch (IOException e) {
      LOGGER.error("Failed to process data import kafka record from topic {}", e, record.topic());
      return Future.failedFuture(e);
    }
  }

}
