package org.folio.verticle.consumers;

import static org.folio.services.util.EventHandlingUtil.constructModuleName;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.folio.consumers.AuthorityLinkChunkKafkaHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

public class AuthorityLinkChunkConsumersVerticle extends AbstractVerticle {
  private static AbstractApplicationContext springGlobalContext;

  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();
  public static final String AUTHORITY_INSTANCE_LINKS_TOPIC = "links.instance-authority";

  @Autowired
  private AuthorityLinkChunkKafkaHandler kafkaHandler;

  @Autowired
  private KafkaConfig kafkaConfig;

  @Value("${srs.kafka.AuthorityLinkChunkConsumer.loadLimit:2}")
  private int loadLimit;

  private KafkaConsumerWrapper<String, String> consumer;

  /**
   * @deprecated need to be replaced with spring global context
   * */
  @Deprecated(forRemoval = false)
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    AuthorityLinkChunkConsumersVerticle.springGlobalContext = springGlobalContext;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    context.put("springContext", springGlobalContext);

    SpringContextUtil.autowireDependencies(this, context);

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
      .createSubscriptionDefinition(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(),
        AUTHORITY_INSTANCE_LINKS_TOPIC);

    consumer = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(globalLoadSensor)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    consumer.start(kafkaHandler, constructModuleName() + "_" + getClass().getSimpleName())
      .onComplete(ar -> startPromise.complete());
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.stop().onComplete(ar -> stopPromise.complete());
  }

}
