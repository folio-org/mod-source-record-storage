package org.folio.verticle.consumers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import java.util.List;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(SCOPE_PROTOTYPE)
public class ParsedRecordChunkConsumersVerticle extends AbstractConsumerVerticle {

  private final AsyncRecordHandler<String, String> parsedRecordChunksKafkaHandler;

  private final ProcessRecordErrorHandler<String, String> parsedRecordChunksErrorHandler;

  @Value("${srs.kafka.ParsedMarcChunkConsumer.loadLimit:5}")
  private int loadLimit;

  @Autowired
  protected ParsedRecordChunkConsumersVerticle(KafkaConfig kafkaConfig,
                                               @Qualifier("parsedRecordChunksKafkaHandler")
                                               AsyncRecordHandler<String, String> parsedRecordChunksKafkaHandler,
                                               @Qualifier("parsedRecordChunksErrorHandler")
                                               ProcessRecordErrorHandler<String, String> parsedRecordChunksErrorHandler) {
    super(kafkaConfig);
    this.parsedRecordChunksKafkaHandler = parsedRecordChunksKafkaHandler;
    this.parsedRecordChunksErrorHandler = parsedRecordChunksErrorHandler;
  }

  @Override
  protected ProcessRecordErrorHandler<String, String> processRecordErrorHandler() {
    return parsedRecordChunksErrorHandler;
  }

  @Override
  protected int loadLimit() {
    return loadLimit;
  }

  @Override
  protected AsyncRecordHandler<String, String> recordHandler() {
    return parsedRecordChunksKafkaHandler;
  }

  @Override
  protected List<String> eventTypes() {
    return List.of(DI_RAW_RECORDS_CHUNK_PARSED.value());
  }

}
