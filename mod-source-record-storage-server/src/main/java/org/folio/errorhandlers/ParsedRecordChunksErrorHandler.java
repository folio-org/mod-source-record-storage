package org.folio.errorhandlers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.*;
import org.folio.services.util.EventHandlingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
public class ParsedRecordChunksErrorHandler implements ProcessRecordErrorHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String ERROR_KEY = "ERROR";
  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String RECORD_ID_HEADER = "recordId";

  @Autowired
  private KafkaConfig kafkaConfig;
  @Autowired
  private Vertx vertx;

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, String> record) {
    Event event = Json.decodeValue(record.value(), Event.class);
    RecordCollection recordCollection = Json.decodeValue(event.getEventPayload(), RecordCollection.class);

    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);

    String jobExecutionId = okapiConnectionParams.getHeaders().get(JOB_EXECUTION_ID_HEADER);
    String tenantId = okapiConnectionParams.getTenantId();

    sendErrorRecordsSavingEvents(recordCollection, throwable.getMessage(), kafkaHeaders, jobExecutionId, tenantId);
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

    GenericCompositeFuture.join(sendingFutures)
      .onFailure(th -> LOGGER.warn("Failed to send DI_ERROR events for failure processed parsed record chunks" , th));
  }
}
