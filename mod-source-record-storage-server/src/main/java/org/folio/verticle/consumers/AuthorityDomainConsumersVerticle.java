package org.folio.verticle.consumers;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import java.util.List;
import java.util.Optional;
import org.folio.AuthorityDomainKafkaTopic;
import org.folio.consumers.AuthorityDomainKafkaHandler;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(SCOPE_PROTOTYPE)
public class AuthorityDomainConsumersVerticle extends AbstractConsumerVerticle {

  private final AuthorityDomainKafkaHandler kafkaHandler;

  @Value("${srs.kafka.AuthorityDomainConsumer.loadLimit:10}")
  private int loadLimit;

  @Autowired
  protected AuthorityDomainConsumersVerticle(KafkaConfig kafkaConfig, AuthorityDomainKafkaHandler kafkaHandler) {
    super(kafkaConfig);
    this.kafkaHandler = kafkaHandler;
  }

  @Override
  protected int loadLimit() {
    return loadLimit;
  }

  @Override
  protected Optional<String> namespace() {
    return Optional.empty();
  }

  @Override
  protected AsyncRecordHandler<String, String> recordHandler() {
    return kafkaHandler;
  }

  @Override
  protected List<String> eventTypes() {
    return List.of(AuthorityDomainKafkaTopic.AUTHORITY.moduleTopicName());
  }
}
