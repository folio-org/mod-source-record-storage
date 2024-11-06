package org.folio.verticle.consumers;

import static org.folio.EntityLinksKafkaTopic.INSTANCE_AUTHORITY;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

import java.util.List;
import java.util.Optional;
import org.folio.consumers.AuthorityLinkChunkKafkaHandler;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(SCOPE_PROTOTYPE)
public class AuthorityLinkChunkConsumersVerticle extends AbstractConsumerVerticle<String, String> {

  private final AuthorityLinkChunkKafkaHandler kafkaHandler;

  private static final int AUTHORITY_LINK_CHUNK_CONSUMER_LOAD_LIMIT = 1;

  @Autowired
  public AuthorityLinkChunkConsumersVerticle(KafkaConfig kafkaConfig, AuthorityLinkChunkKafkaHandler kafkaHandler) {
    super(kafkaConfig);
    this.kafkaHandler = kafkaHandler;
  }

  @Override
  protected int loadLimit() {
    return AUTHORITY_LINK_CHUNK_CONSUMER_LOAD_LIMIT;
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
    return List.of(INSTANCE_AUTHORITY.moduleTopicName());
  }
}
