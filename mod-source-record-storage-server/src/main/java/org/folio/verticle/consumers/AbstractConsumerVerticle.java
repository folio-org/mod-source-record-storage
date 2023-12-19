package org.folio.verticle.consumers;

import static org.folio.kafka.KafkaTopicNameHelper.createSubscriptionDefinition;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.services.util.EventHandlingUtil.constructModuleName;
import static org.folio.services.util.EventHandlingUtil.createSubscriptionPattern;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.okapi.common.GenericCompositeFuture;

public abstract class AbstractConsumerVerticle extends AbstractVerticle {

  private final List<KafkaConsumerWrapper<String, String>> consumers = new ArrayList<>();

  private final KafkaConfig kafkaConfig;

  protected AbstractConsumerVerticle(KafkaConfig kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    eventTypes().forEach(eventType -> {
      SubscriptionDefinition subscriptionDefinition = getSubscriptionDefinition(eventType);
      consumers.add(KafkaConsumerWrapper.<String, String>builder()
        .context(context)
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit())
        .globalLoadSensor(new GlobalLoadSensor())
        .subscriptionDefinition(subscriptionDefinition)
        .processRecordErrorHandler(processRecordErrorHandler())
        .build());
    });

    List<Future<Void>> futures = new ArrayList<>();
    consumers.forEach(consumer -> futures.add(consumer.start(recordHandler(), getConsumerName())));

    GenericCompositeFuture.all(futures).onComplete(ar -> startPromise.complete());
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    List<Future<Void>> futures = new ArrayList<>();
    consumers.forEach(consumerWrapper -> futures.add(consumerWrapper.stop()));

    GenericCompositeFuture.join(futures).onComplete(ar -> stopPromise.complete());
  }

  protected abstract int loadLimit();

  protected Optional<String> namespace() {
    return Optional.of(getDefaultNameSpace());
  }

  protected abstract AsyncRecordHandler<String, String> recordHandler();

  protected ProcessRecordErrorHandler<String, String> processRecordErrorHandler() {
    return null;
  }

  protected abstract List<String> eventTypes();

  private SubscriptionDefinition getSubscriptionDefinition(String eventType) {
    SubscriptionDefinition subscriptionDefinition;
    var namespace = namespace();
    if (namespace.isPresent()) {
      subscriptionDefinition = createSubscriptionDefinition(kafkaConfig.getEnvId(), namespace.get(), eventType);
    } else {
      subscriptionDefinition = SubscriptionDefinition.builder()
        .eventType(eventType)
        .subscriptionPattern(createSubscriptionPattern(kafkaConfig.getEnvId(), eventType))
        .build();
    }
    return subscriptionDefinition;
  }

  private String getConsumerName() {
    return constructModuleName() + "_" + getClass().getSimpleName();
  }
}
