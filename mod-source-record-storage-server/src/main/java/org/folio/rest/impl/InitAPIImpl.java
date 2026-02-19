package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.spi.VerticleFactory;
import java.util.List;
import java.util.OptionalInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.kafka.KafkaConfig;
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

import static io.vertx.core.ThreadingModel.EVENT_LOOP;
import static io.vertx.core.ThreadingModel.WORKER;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private KafkaConfig kafkaConfig;

  @Autowired
  private List<EventHandler> eventHandlers;

  @Autowired
  private SpringVerticleFactory verticleFactory;

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
      vertx.registerVerticleFactory(verticleFactory);

      EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, maxDistributionNumber);

      registerEventHandlers();
      deployMarcIndexersVersionDeletionVerticle(vertx, verticleFactory);
      deployConsumerVerticles(vertx).onComplete(ar -> {
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

  private Future<?> deployConsumerVerticles(Vertx vertx) {
    return Future.all(List.of(
      deployWorkerVerticle(vertx, AuthorityLinkChunkConsumersVerticle.class,
        OptionalInt.of(authorityLinkChunkConsumerInstancesNumber)),
      deployWorkerVerticle(vertx, AuthorityDomainConsumersVerticle.class,
        OptionalInt.of(authorityDomainConsumerInstancesNumber)),
      deployWorkerVerticle(vertx, DataImportConsumersVerticle.class,
        OptionalInt.of(dataImportConsumerInstancesNumber)),
      deployWorkerVerticle(vertx, ParsedRecordChunkConsumersVerticle.class,
        OptionalInt.of(parsedMarcChunkConsumerInstancesNumber)),
      deployWorkerVerticle(vertx, QuickMarcConsumersVerticle.class,
        OptionalInt.of(quickMarcConsumerInstancesNumber)),
      deployVerticle(vertx, CancelledJobExecutionConsumersVerticle.class, OptionalInt.of(1), EVENT_LOOP)
    ));
  }

  private <T> String getVerticleName(VerticleFactory verticleFactory, Class<T> clazz) {
    return verticleFactory.prefix() + ":" + clazz.getName();
  }

  private void deployMarcIndexersVersionDeletionVerticle(Vertx vertx, VerticleFactory verticleFactory) {
    vertx.deployVerticle(getVerticleName(verticleFactory, (Class<?>) MarcIndexersVersionDeletionVerticle.class),
      new DeploymentOptions().setThreadingModel(WORKER));
  }

  private Future<String> deployWorkerVerticle(Vertx vertx, Class<?> verticleClass, OptionalInt instancesNumber) {
    return deployVerticle(vertx, verticleClass, instancesNumber, WORKER);
  }

  private Future<String> deployVerticle(Vertx vertx, Class<?> verticleClass, OptionalInt instancesNumber,
                                        ThreadingModel threadingModel) {
    return vertx.deployVerticle(getVerticleName(verticleFactory, verticleClass),
      new DeploymentOptions().setThreadingModel(threadingModel).setInstances(instancesNumber.orElse(1)));
  }

}
