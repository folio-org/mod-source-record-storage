package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.spi.VerticleFactory;
import java.util.List;
import java.util.OptionalInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.kafka.KafkaConfig;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.MarcIndexersVersionDeletionVerticle;
import org.folio.verticle.SpringVerticleFactory;
import org.folio.verticle.consumers.AuthorityDomainConsumersVerticle;
import org.folio.verticle.consumers.AuthorityLinkChunkConsumersVerticle;
import org.folio.verticle.consumers.CancelledJobExecutionConsumersVerticle;
import org.folio.verticle.consumers.DataImportConsumersVerticle;
import org.folio.verticle.consumers.ParsedRecordChunkConsumersVerticle;
import org.folio.verticle.consumers.QuickMarcConsumersVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

import static io.vertx.core.ThreadingModel.EVENT_LOOP;
import static io.vertx.core.ThreadingModel.WORKER;

public class InitAPIImpl implements InitAPI {

  private static final String SPRING_CONTEXT = "springContext";
  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private KafkaConfig kafkaConfig;

  @Autowired
  private List<EventHandler> eventHandlers;

  @Value("${srs.kafka.ParsedMarcChunkConsumer.instancesNumber:1}")
  private int parsedMarcChunkConsumerInstancesNumber;

  @Value("${srs.kafka.DataImportConsumer.instancesNumber:1}")
  private int dataImportConsumerInstancesNumber;

  @Value("${srs.kafka.QuickMarcConsumer.instancesNumber:1}")
  private int quickMarcConsumerInstancesNumber;

  @Value("${srs.kafka.AuthorityLinkChunkConsumer.instancesNumber:1}")
  private int authorityLinkChunkConsumerInstancesNumber;

  @Value("${srs.kafka.AuthorityDomainConsumer.instancesNumber:1}")
  private int authorityDomainConsumerInstancesNumber;

  @Value("${srs.kafka.DataImportConsumerVerticle.maxDistributionNum:100}")
  private int maxDistributionNumber;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      AbstractApplicationContext springContext = vertx.getOrCreateContext().get(SPRING_CONTEXT);
      VerticleFactory verticleFactory = springContext.getBean(SpringVerticleFactory.class);
      vertx.registerVerticleFactory(verticleFactory);

      EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, maxDistributionNumber);

      registerEventHandlers();
      deployMarcIndexersVersionDeletionVerticle(vertx, verticleFactory);
      deployConsumerVerticles(vertx, verticleFactory).onComplete(ar -> {
        if (ar.succeeded()) {
          handler.handle(Future.succeededFuture(true));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    } catch (Exception th) {
      LOGGER.error("init:: Failed to init module", th);
      handler.handle(Future.failedFuture(th));
    }
  }

  private void registerEventHandlers() {
    eventHandlers.forEach(EventManager::registerEventHandler);
  }

  private Future<?> deployConsumerVerticles(Vertx vertx, VerticleFactory verticleFactory) {
    Promise<String> deployConsumer1 = Promise.promise();
    Promise<String> deployConsumer2 = Promise.promise();
    Promise<String> deployConsumer3 = Promise.promise();
    Promise<String> deployConsumer4 = Promise.promise();
    Promise<String> deployConsumer5 = Promise.promise();
    Promise<String> deployConsumer6 = Promise.promise();

    deployWorkerVerticle(vertx, verticleFactory, AuthorityLinkChunkConsumersVerticle.class,
      OptionalInt.of(authorityLinkChunkConsumerInstancesNumber), deployConsumer1);
    deployWorkerVerticle(vertx, verticleFactory, AuthorityDomainConsumersVerticle.class,
      OptionalInt.of(authorityDomainConsumerInstancesNumber), deployConsumer2);
    deployWorkerVerticle(vertx, verticleFactory, DataImportConsumersVerticle.class,
      OptionalInt.of(dataImportConsumerInstancesNumber), deployConsumer3);
    deployWorkerVerticle(vertx, verticleFactory, ParsedRecordChunkConsumersVerticle.class,
      OptionalInt.of(parsedMarcChunkConsumerInstancesNumber), deployConsumer4);
    deployWorkerVerticle(vertx, verticleFactory, QuickMarcConsumersVerticle.class,
      OptionalInt.of(quickMarcConsumerInstancesNumber), deployConsumer5);
    deployVerticle(vertx, verticleFactory, CancelledJobExecutionConsumersVerticle.class,
      OptionalInt.of(1), EVENT_LOOP, deployConsumer6);

    return GenericCompositeFuture.all(List.of(
      deployConsumer1.future(),
      deployConsumer2.future(),
      deployConsumer3.future(),
      deployConsumer4.future(),
      deployConsumer5.future(),
      deployConsumer6.future()
    ));
  }

  private <T> String getVerticleName(VerticleFactory verticleFactory, Class<T> clazz) {
    return verticleFactory.prefix() + ":" + clazz.getName();
  }

  private void deployMarcIndexersVersionDeletionVerticle(Vertx vertx, VerticleFactory verticleFactory) {
    vertx.deployVerticle(getVerticleName(verticleFactory, (Class<?>) MarcIndexersVersionDeletionVerticle.class),
      new DeploymentOptions().setThreadingModel(WORKER));
  }

  private void deployWorkerVerticle(Vertx vertx, VerticleFactory verticleFactory, Class<?> verticleClass,
                              OptionalInt instancesNumber, Promise<String> promise) {
    deployVerticle(vertx, verticleFactory, verticleClass, instancesNumber, WORKER, promise);
  }

  private void deployVerticle(Vertx vertx, VerticleFactory verticleFactory, Class<?> verticleClass,
                              OptionalInt instancesNumber, ThreadingModel threadingModel, Promise<String> promise) {
    vertx.deployVerticle(getVerticleName(verticleFactory, verticleClass),
      new DeploymentOptions().setThreadingModel(threadingModel).setInstances(instancesNumber.orElse(1)), promise);
  }

}
