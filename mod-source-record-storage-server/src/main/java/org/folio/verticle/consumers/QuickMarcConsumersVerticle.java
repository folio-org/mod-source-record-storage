package org.folio.verticle.consumers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.rest.tools.utils.ModuleName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

import org.folio.dao.util.QMEventTypes;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.services.QuickMarcKafkaHandler;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;

import static org.folio.services.util.EventHandlingUtil.constructModelName;

public class QuickMarcConsumersVerticle extends AbstractVerticle {

  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  private static AbstractApplicationContext springGlobalContext;

  @Autowired
  private QuickMarcKafkaHandler kafkaHandler;

  @Autowired
  private KafkaConfig kafkaConfig;

  @Value("${srs.kafka.QuickMarcConsumer.loadLimit:5}")
  private int loadLimit;

  @Value("${srs.kafka.QuickMarcConsumerVerticle.maxDistributionNum:100}")
  private int maxDistributionNumber;

  private KafkaConsumerWrapper<String, String> consumer;

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    QuickMarcConsumersVerticle.springGlobalContext = springGlobalContext;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    context.put("springContext", springGlobalContext);

    SpringContextUtil.autowireDependencies(this, context);

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
      .createSubscriptionDefinition(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(),
        QMEventTypes.QM_RECORD_UPDATED.name());

    consumer = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(globalLoadSensor)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    consumer.start(kafkaHandler, constructModelName() + "_" + getClass().getSimpleName())
      .onComplete(ar -> startPromise.complete());
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumer.stop().onComplete(ar -> stopPromise.complete());
  }
}
