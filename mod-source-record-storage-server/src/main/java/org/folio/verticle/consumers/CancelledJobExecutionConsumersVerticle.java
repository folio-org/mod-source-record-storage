package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.headers.FolioKafkaHeaders;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;
import static org.folio.services.util.KafkaUtil.extractHeaderValue;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

@Component
@Scope(SCOPE_PROTOTYPE)
public class CancelledJobExecutionConsumersVerticle extends AbstractConsumerVerticle<String, byte[]>
  implements AsyncRecordHandler<String, byte[]> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final CancelledJobsIdsCache cancelledJobsIdsCache;
  private final int loadLimit;

  @Autowired
  public CancelledJobExecutionConsumersVerticle(CancelledJobsIdsCache cancelledJobsIdsCache, KafkaConfig kafkaConfig,
    @Value("${srs.kafka.CancelledJobExecutionConsumer.loadLimit:1000}") int loadLimit) {
    super(kafkaConfig);
    this.cancelledJobsIdsCache = cancelledJobsIdsCache;
    this.loadLimit = loadLimit;
  }

  @Override
  protected int loadLimit() {
    return loadLimit;
  }

  @Override
  protected AsyncRecordHandler<String, byte[]> recordHandler() {
    return this;
  }

  @Override
  protected List<String> eventTypes() {
    return List.of(DI_JOB_CANCELLED.value());
  }

  @Override
  public String getDeserializerClass() {
    return ByteArrayDeserializer.class.getName();
  }

  /**
   * Constructs a unique module name with pseudo-random suffix.
   * This ensures that each instance of the module will have own consumer group
   * and will consume all messages from the topic.
   *
   * @return unique module name string.
   */
  @Override
  protected String getModuleName() {
    return ModuleName.getModuleName() + "-" + java.util.UUID.randomUUID();
  }

  @Override
  @SuppressWarnings("squid:S2629")
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> kafkaRecord) {
    try {
      String tenantId = extractHeaderValue(FolioKafkaHeaders.TENANT_ID, kafkaRecord.headers());
      LOGGER.debug("handle:: Received cancelled job event, key: '{}', tenantId: '{}'", kafkaRecord.key(), tenantId);

      String jobId = DatabindCodec.mapper().readValue(kafkaRecord.value(), Event.class).getEventPayload();
      cancelledJobsIdsCache.put(jobId);
      LOGGER.info("handle:: Processed cancelled job, jobId: '{}', tenantId: '{}', topic: '{}'",
        jobId, tenantId, kafkaRecord.topic());
      return Future.succeededFuture(kafkaRecord.key());
    } catch (Exception e) {
      LOGGER.warn("handle:: Failed to process cancelled job, key: '{}', from topic: '{}'",
        kafkaRecord.key(), kafkaRecord.topic(), e);
      return Future.failedFuture(e);
    }
  }

}
