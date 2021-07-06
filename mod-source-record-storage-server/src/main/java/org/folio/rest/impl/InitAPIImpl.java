package org.folio.rest.impl;

import java.util.List;

import io.vertx.core.AsyncResult;
import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.processing.events.EventManager;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.handlers.InstancePostProcessingEventHandler;
import org.folio.services.handlers.MarcAuthorityEventHandler;
import org.folio.services.handlers.MarcBibliographicMatchEventHandler;
import org.folio.services.handlers.actions.ModifyRecordEventHandler;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.consumers.DataImportConsumersVerticle;
import org.folio.verticle.consumers.ParsedRecordChunkConsumersVerticle;
import org.folio.verticle.consumers.QuickMarcConsumersVerticle;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private InstancePostProcessingEventHandler instancePostProcessingEventHandler;

  @Autowired
  private ModifyRecordEventHandler modifyRecordEventHandler;

  @Autowired
  private MarcBibliographicMatchEventHandler marcBibliographicMatchEventHandler;

  @Autowired
  private MarcAuthorityEventHandler marcAuthorityEventHandler;

  @Value("${srs.kafka.ParsedMarcChunkConsumer.instancesNumber:1}")
  private int parsedMarcChunkConsumerInstancesNumber;

  @Value("${srs.kafka.DataImportConsumer.instancesNumber:1}")
  private int dataImportConsumerInstancesNumber;

  @Value("${srs.kafka.QuickMarcConsumer.instancesNumber:1}")
  private int quickMarcConsumerInstancesNumber;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      registerEventHandlers();
      deployConsumerVerticles(vertx).onComplete(ar -> {
        if (ar.succeeded()) {
          handler.handle(Future.succeededFuture(true));
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    } catch (Throwable th) {
      LOGGER.error("Failed to init module", th);
      handler.handle(Future.failedFuture(th));
    }
  }

  private void registerEventHandlers() {
    EventManager.registerEventHandler(instancePostProcessingEventHandler);
    EventManager.registerEventHandler(modifyRecordEventHandler);
    EventManager.registerEventHandler(marcBibliographicMatchEventHandler);
    EventManager.registerEventHandler(marcAuthorityEventHandler);
  }

  private Future<?> deployConsumerVerticles(Vertx vertx) {
    //TODO: get rid of this workaround with global spring context
    ParsedRecordChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    DataImportConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    QuickMarcConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));

    Promise<String> deployConsumer1 = Promise.promise();
    Promise<String> deployConsumer2 = Promise.promise();
    Promise<String> deployConsumer3 = Promise.promise();

    vertx.deployVerticle(ParsedRecordChunkConsumersVerticle.class.getCanonicalName(),
      new DeploymentOptions().setWorker(true).setInstances(parsedMarcChunkConsumerInstancesNumber), deployConsumer1);

    vertx.deployVerticle(DataImportConsumersVerticle.class.getCanonicalName(),
      new DeploymentOptions().setWorker(true).setInstances(dataImportConsumerInstancesNumber), deployConsumer2);

    vertx.deployVerticle(QuickMarcConsumersVerticle.class.getCanonicalName(),
      new DeploymentOptions().setWorker(true).setInstances(quickMarcConsumerInstancesNumber), deployConsumer3);

    return GenericCompositeFuture.all(List.of(deployConsumer1.future(), deployConsumer2.future(), deployConsumer3.future()));
  }

}
