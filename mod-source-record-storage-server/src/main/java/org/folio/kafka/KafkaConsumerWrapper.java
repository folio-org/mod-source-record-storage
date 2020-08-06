package org.folio.kafka;

import com.google.common.base.Strings;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import lombok.Builder;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class KafkaConsumerWrapper<K, V> implements Handler<KafkaConsumerRecord<K, V>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWrapper.class);
  private final static AtomicInteger indexer = new AtomicInteger();

  private final int id = indexer.getAndIncrement();

  private final AtomicInteger localLoadSensor = new AtomicInteger();

  private final AtomicInteger pauseRequests = new AtomicInteger();

  private final Vertx vertx;

  private final Context context;

  private final KafkaConfig kafkaConfig;

  private final SubscriptionDefinition subscriptionDefinition;

  private final GlobalLoadSensor globalLoadSensor;

  private AsyncRecordHandler<K, V> businessHandler;

  private int loadLimit;

  private int loadBottomGreenLine;

  private KafkaConsumer<K, V> kafkaConsumer;

  public int getLoadLimit() {
    return loadLimit;
  }

  public void setLoadLimit(int loadLimit) {
    this.loadLimit = loadLimit;
    this.loadBottomGreenLine = loadLimit / 2;
  }

  @Builder
  private KafkaConsumerWrapper(Vertx vertx, Context context, KafkaConfig kafkaConfig, SubscriptionDefinition subscriptionDefinition, GlobalLoadSensor globalLoadSensor, int loadLimit) {
    this.vertx = vertx;
    this.context = context;
    this.kafkaConfig = kafkaConfig;
    this.subscriptionDefinition = subscriptionDefinition;
    this.globalLoadSensor = globalLoadSensor;
    this.loadLimit = loadLimit;
  }

  public Future<Void> start(AsyncRecordHandler<K, V> businessHandler) {
    if (Objects.isNull(businessHandler)) {
      String failureMessage = "businessHandler must be provided and can't be null.";
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    if (Objects.isNull(subscriptionDefinition) || Strings.isNullOrEmpty(subscriptionDefinition.getSubscriptionPattern())) {
      String failureMessage = "subscriptionPattern can't be null nor empty. " + subscriptionDefinition;
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    if (loadLimit < 1) {
      String failureMessage = "loadLimit must be greater than 0. Current value is " + loadLimit;
      LOGGER.error(failureMessage);
      return Future.failedFuture(failureMessage);
    }

    this.businessHandler = businessHandler;
    Promise<Void> startPromise = Promise.promise();

    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaTopicNameHelper.formatGroupName(subscriptionDefinition.getEventType()));

    kafkaConsumer = KafkaConsumer.create(vertx, consumerProps);

    kafkaConsumer.handler(this);

    Pattern pattern = Pattern.compile(subscriptionDefinition.getSubscriptionPattern());
    kafkaConsumer.subscribe(pattern, ar -> {
      if (ar.succeeded()) {
        LOGGER.info("Consumer created - id: " + id + " subscriptionPattern: " + subscriptionDefinition);
        startPromise.complete();
      } else {
        ar.cause().printStackTrace();
        startPromise.fail(ar.cause());
      }
    });

    return startPromise.future();
  }

  public Future<Void> stop() {
    Promise<Void> stopPromise = Promise.promise();
    kafkaConsumer.unsubscribe(uar -> {
        if (uar.succeeded()) {
          LOGGER.info("Consumer unsubscribed - id: " + id + " subscriptionPattern: " + subscriptionDefinition);
        } else {
          LOGGER.error("Consumer not unsubscribed - id: " + id + " subscriptionPattern: " + subscriptionDefinition + "\n" + uar.cause());
        }
        kafkaConsumer.close(car -> {
          if (uar.succeeded()) {
            LOGGER.info("Consumer closed - id: " + id + " subscriptionPattern: " + subscriptionDefinition);
          } else {
            LOGGER.error("Consumer not closed - id: " + id + " subscriptionPattern: " + subscriptionDefinition + "\n" + car.cause());
          }
          stopPromise.complete();
        });
      }
    );

    return stopPromise.future();
  }

  @Override
  public void handle(KafkaConsumerRecord<K, V> record) {
    if (Objects.nonNull(globalLoadSensor)) {
      globalLoadSensor.increment();
    }
    int currentLoad = localLoadSensor.incrementAndGet();

    if (currentLoad > loadLimit) {
      int requestNo = pauseRequests.getAndIncrement();
      if (requestNo == 0) {
//        synchronized (this) {
        kafkaConsumer.pause();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Consumer - id: " + id + " subscriptionPattern: " + subscriptionDefinition + " kafkaConsumer.pause() requested");
        }
//        }
      }
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Consumer - id: " + id +
        " subscriptionPattern: " + subscriptionDefinition +
        " a Record has been received. key: " + record.key() +
        " currentLoad: " + currentLoad +
        " globalLoad: " + (Objects.nonNull(globalLoadSensor) ? String.valueOf(globalLoadSensor.current()) : "N/A"));
    }

    businessHandler.handle(record).onComplete(har -> {
      try {

        long offset = record.offset() + 1;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(2);
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, null);
        offsets.put(topicPartition, offsetAndMetadata);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Consumer - id: " + id + " subscriptionPattern: " + subscriptionDefinition + " Committing offset: " + offset);
        }
        kafkaConsumer.commit(offsets, completionHandler -> {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Consumer - id: " + id + " subscriptionPattern: " + subscriptionDefinition + " Committed offset: " + offset);
          }
        });

        if (har.failed()) {
          LOGGER.error("Error while processing a record - id: " + id + " subscriptionPattern: " + subscriptionDefinition + "\n" + har.cause());
          //TODO: add smart error handling here
        }
      } finally {
        int actualCurrentLoad = localLoadSensor.decrementAndGet();
        globalLoadSensor.decrement();

        if (actualCurrentLoad <= loadBottomGreenLine || actualCurrentLoad == 0) {
          int requestNo = pauseRequests.decrementAndGet();
          if (requestNo == 0) {
//           synchronized (this) {
            kafkaConsumer.resume();
            LOGGER.debug("Consumer - id: " + id + " subscriptionPattern: " + subscriptionDefinition + " kafkaConsumer.resume() requested");
//            }
          }
        }
      }

    });

  }
}
