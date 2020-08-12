package org.folio.verticle.consumers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

public class ParsedMarcChunkConsumersVerticle extends AbstractVerticle {
  //TODO: get rid of this workaround with global spring context
  private static AbstractApplicationContext springGlobalContext;

  //TODO: make it an ENUM value
  public static final String DI_RAWMARCS_CHUNK_PARSED_EVENT_TYPE = "DI_RAWMARCS_CHUNK_PARSED";

  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  @Autowired
  @Qualifier("ParsedMarcChunksKafkaHandler")
  private AsyncRecordHandler<String, String> parsedMarcChunksKafkaHandler;

  @Autowired
  private KafkaConfig kafkaConfig;

  @Value("${srs.kafka.ParsedMarcChunkConsumer.loadLimit:5}")
  private int loadLimit;

  private KafkaConsumerWrapper<String, String> consumerWrapper;

  @Override
  public void start(Promise<Void> startPromise) {
    context.put("springContext", springGlobalContext);

    SpringContextUtil.autowireDependencies(this, context);

    SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper.createSubscriptionDefinition(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), DI_RAWMARCS_CHUNK_PARSED_EVENT_TYPE);

    consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
      .context(context)
      .vertx(vertx)
      .kafkaConfig(kafkaConfig)
      .loadLimit(loadLimit)
      .globalLoadSensor(globalLoadSensor)
      .subscriptionDefinition(subscriptionDefinition)
      .build();

    consumerWrapper.start(parsedMarcChunksKafkaHandler).onComplete(sar -> {
      if (sar.succeeded()) {
        startPromise.complete();
      } else {
        startPromise.fail(sar.cause());
      }
    });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumerWrapper.stop().onComplete(ar -> stopPromise.complete());
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    ParsedMarcChunkConsumersVerticle.springGlobalContext = springGlobalContext;
  }

}
