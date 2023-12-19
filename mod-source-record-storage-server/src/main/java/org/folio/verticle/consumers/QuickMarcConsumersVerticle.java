package org.folio.verticle.consumers;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import java.util.List;
import org.folio.consumers.QuickMarcKafkaHandler;
import org.folio.dao.util.QMEventTypes;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(SCOPE_PROTOTYPE)
public class QuickMarcConsumersVerticle extends AbstractConsumerVerticle {

  private final QuickMarcKafkaHandler kafkaHandler;

  @Value("${srs.kafka.QuickMarcConsumer.loadLimit:5}")
  private int loadLimit;

  @Autowired
  protected QuickMarcConsumersVerticle(KafkaConfig kafkaConfig, QuickMarcKafkaHandler kafkaHandler) {
    super(kafkaConfig);
    this.kafkaHandler = kafkaHandler;
  }

  @Override
  protected int loadLimit() {
    return loadLimit;
  }

  @Override
  protected AsyncRecordHandler<String, String> recordHandler() {
    return kafkaHandler;
  }

  @Override
  protected List<String> eventTypes() {
    return List.of(QMEventTypes.QM_RECORD_UPDATED.name());
  }
}
