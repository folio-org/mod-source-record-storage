package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.processing.events.EventManager;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_ERROR;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String CORRELATION_ID_HEADER = "correlationId";
  private static final String PROFILE_SNAPSHOT_ID_KEY = "profileSnapshotId";

  private Vertx vertx;
  private JobProfileSnapshotCache profileSnapshotCache;

  @Autowired
  public DataImportKafkaHandler(Vertx vertx, JobProfileSnapshotCache profileSnapshotCache) {
    this.vertx = vertx;
    this.profileSnapshotCache = profileSnapshotCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      String correlationId = extractCorrelationId(record.headers());
      Event event = ObjectMapperTool.getMapper().readValue(record.value(), Event.class);
      DataImportEventPayload eventPayload = Json.decodeValue(event.getEventPayload(), DataImportEventPayload.class);
      LOGGER.debug("Data import event payload has been received with event type: {} and correlationId: {}", eventPayload.getEventType(), correlationId);
      eventPayload.getContext().put(CORRELATION_ID_HEADER, correlationId);

      OkapiConnectionParams params = retrieveOkapiConnectionParams(eventPayload);
      String jobProfileSnapshotId = eventPayload.getContext().get(PROFILE_SNAPSHOT_ID_KEY);
      profileSnapshotCache.get(jobProfileSnapshotId, params)
        .toCompletionStage()
        .thenCompose(snapshotOptional -> snapshotOptional
          .map(profileSnapshot -> EventManager.handleEvent(eventPayload, profileSnapshot))
          .orElse(CompletableFuture.failedFuture(new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", jobProfileSnapshotId)))))
        .whenComplete((processedPayload, throwable) -> {
        if (throwable != null) {
          promise.fail(throwable);
        } else if (DI_ERROR.value().equals(processedPayload.getEventType())) {
          promise.fail("Failed to process data import event payload");
        } else {
          promise.complete(record.key());
        }
      });
      return promise.future();
    } catch (Exception e) {
      LOGGER.error("Failed to process data import kafka record from topic {}", record.topic(), e);
      return Future.failedFuture(e);
    }
  }

  private String extractCorrelationId(List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equals(CORRELATION_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }

  private OkapiConnectionParams retrieveOkapiConnectionParams(DataImportEventPayload eventPayload) {
    return new OkapiConnectionParams(Map.of(
      RestUtil.OKAPI_URL_HEADER, eventPayload.getOkapiUrl(),
      RestUtil.OKAPI_TENANT_HEADER, eventPayload.getTenant(),
      RestUtil.OKAPI_TOKEN_HEADER, eventPayload.getToken()
    ), this.vertx);
  }

}
