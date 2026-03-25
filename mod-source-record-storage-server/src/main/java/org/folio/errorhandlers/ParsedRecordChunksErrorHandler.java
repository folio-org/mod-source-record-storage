package org.folio.errorhandlers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.util.EventHandlingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;

/**
 * This error handler executed in folio-kafka-wrapper library,
 * @see org.folio.kafka.KafkaConsumerWrapper
 * in cases for failed futures.
 * This handler sends DI_ERROR event, mod-source-record-manager accepts these events and stops progress in order to finish import
 * with status 'Completed with errors' with showing error messge instead of hanging progress bar.
 */
@Component
public class ParsedRecordChunksErrorHandler implements ProcessRecordErrorHandler<String, byte[]> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String ERROR_KEY = "ERROR";
  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String CORRELATION_ID_HEADER = "correlationId";
  public static final String RECORD_ID_HEADER = "recordId";

  @Autowired
  private KafkaConfig kafkaConfig;
  @Autowired
  private Vertx vertx;

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, byte[]> consumerRecord) {
    LOGGER.trace("handle:: Handling record {}", consumerRecord);
      Event event;
      try {
          event = DatabindCodec.mapper().readValue(consumerRecord.value(), Event.class);
      } catch (IOException e) {
          LOGGER.error("Something happened when deserializing record", e);
          return;
      }
      RecordCollection recordCollection = Json.decodeValue(event.getEventPayload(), RecordCollection.class);

    List<KafkaHeader> kafkaHeaders = consumerRecord.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);

    String jobExecutionId = okapiConnectionParams.getHeaders().get(JOB_EXECUTION_ID_HEADER);
    String correlationId = okapiConnectionParams.getHeaders().get(CORRELATION_ID_HEADER);
    String tenantId = okapiConnectionParams.getTenantId();

    if(throwable instanceof DuplicateEventException) {
      LOGGER.warn("handle:: Duplicate event received, skipping processing for jobExecutionId: {} , tenantId: {}, correlationId:{}, totalRecords: {}, cause: {}", jobExecutionId, tenantId, correlationId, recordCollection.getTotalRecords(), throwable.getMessage());
    } else {
      sendErrorRecordsSavingEvents(recordCollection, throwable.getMessage(), kafkaHeaders, jobExecutionId, tenantId);
    }
  }

  private void sendErrorRecordsSavingEvents(RecordCollection recordCollection, String message, List<KafkaHeader> kafkaHeaders, String jobExecutionId, String tenantId) {
    List<Future<Boolean>> sendingFutures = new ArrayList<>();
    for (Record record : recordCollection.getRecords()) {
      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_ERROR.value())
        .withJobExecutionId(jobExecutionId)
        .withEventsChain(List.of(DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value()))
        .withTenant(tenantId)
        .withContext(new HashMap<>(){{
          put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
          put(ERROR_KEY, message);
          put(RECORD_ID_HEADER, record.getId());
        }});

      kafkaHeaders.add(new KafkaHeaderImpl(RECORD_ID_HEADER, record.getId()));

      sendingFutures.add(EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(dataImportEventPayload), DI_ERROR.value(), kafkaHeaders, kafkaConfig, null));
    }

    Future.join(sendingFutures)
      .onFailure(th -> LOGGER.warn("sendErrorRecordsSavingEvents:: Failed to send DI_ERROR events for failure processed parsed record chunks" , th));
  }
}
