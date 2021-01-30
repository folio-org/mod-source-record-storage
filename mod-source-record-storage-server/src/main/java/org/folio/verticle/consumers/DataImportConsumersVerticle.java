package org.folio.verticle.consumers;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.processing.events.EventManager;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;

public class DataImportConsumersVerticle extends AbstractVerticle {
  private static AbstractApplicationContext springGlobalContext;

  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  private final List<String> events = Arrays.asList(DI_SRS_MARC_BIB_RECORD_CREATED.value(),
    DI_INVENTORY_INSTANCE_CREATED.value(), DI_INVENTORY_INSTANCE_UPDATED.value(),
    DI_SRS_MARC_BIB_RECORD_MATCHED.value(), DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value(),
    DI_SRS_MARC_BIB_RECORD_MODIFIED.value(),
    DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value(),
    DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value());

  @Autowired
  @Qualifier("DataImportKafkaHandler")
  private AsyncRecordHandler<String, String> dataImportKafkaHandler;

  @Autowired
  private KafkaConfig kafkaConfig;

  @Value("${srs.kafka.DataImportConsumer.loadLimit:5}")
  private int loadLimit;

  @Value("${srs.kafka.DataImportConsumerVerticle.maxDistributionNum:100}")
  private int maxDistributionNumber;

  private List<KafkaConsumerWrapper<String, String>> consumerWrappersList = new ArrayList<>(events.size());

  @Override
  public void start(Promise<Void> startPromise) {
    context.put("springContext", springGlobalContext);

    SpringContextUtil.autowireDependencies(this, context);

    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, maxDistributionNumber);

    events.forEach(event -> {
      SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
        .createSubscriptionDefinition(kafkaConfig.getEnvId(),
          KafkaTopicNameHelper.getDefaultNameSpace(),
          event);
      consumerWrappersList.add(KafkaConsumerWrapper.<String, String>builder()
        .context(context)
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit)
        .globalLoadSensor(globalLoadSensor)
        .subscriptionDefinition(subscriptionDefinition)
        .build());
    });

    consumerWrappersList.forEach(consumerWrapper -> consumerWrapper
      .start(dataImportKafkaHandler, PubSubClientUtils.constructModuleName())
      .onComplete(ar -> startPromise.complete()));
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumerWrappersList.forEach(consumerWrapper -> consumerWrapper
      .stop()
      .onComplete(ar -> stopPromise.complete()));
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    DataImportConsumersVerticle.springGlobalContext = springGlobalContext;
  }

}
