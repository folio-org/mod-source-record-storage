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
public class ParsedRecordChunkConsumersVerticle extends AbstractConsumerVerticle<String, byte[]> {

  private final AsyncRecordHandler<String, byte[]> parsedRecordChunksKafkaHandler;

  private final ProcessRecordErrorHandler<String, byte[]> parsedRecordChunksErrorHandler;

  @Value("${srs.kafka.ParsedMarcChunkConsumer.loadLimit:5}")
  private int loadLimit;

  @Autowired
  protected ParsedRecordChunkConsumersVerticle(KafkaConfig kafkaConfig,
                                               @Qualifier("parsedRecordChunksKafkaHandler")
                                               AsyncRecordHandler<String, byte[]> parsedRecordChunksKafkaHandler,
                                               @Qualifier("parsedRecordChunksErrorHandler")
                                               ProcessRecordErrorHandler<String, byte[]> parsedRecordChunksErrorHandler) {
    super(kafkaConfig);
    this.parsedRecordChunksKafkaHandler = parsedRecordChunksKafkaHandler;
    this.parsedRecordChunksErrorHandler = parsedRecordChunksErrorHandler;
  }

  @Override
  protected ProcessRecordErrorHandler<String, byte[]> processRecordErrorHandler() {
    return parsedRecordChunksErrorHandler;
  }

  @Override
  protected int loadLimit() {
    return loadLimit;
  }

  @Override
  protected AsyncRecordHandler<String, byte[]> recordHandler() {
    return parsedRecordChunksKafkaHandler;
  }

  @Override
  protected List<String> eventTypes() {
    return List.of(DI_RAW_RECORDS_CHUNK_PARSED.value());
  }

  @Override
  public String getDeserializerClass() {
    return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  }

}
