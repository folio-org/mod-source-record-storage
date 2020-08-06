package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.folio.config.ApplicationConfig;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.consumers.ParsedMarcChunkConsumersVerticle;
import org.springframework.beans.factory.annotation.Value;

public class InitAPIImpl implements InitAPI {

  @Value("${srs.kafka.ParsedMarcChunkConsumer.instancesNumber:1}")
  private int parsedMarcChunkConsumerInstancesNumber;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      deployParsedMarcChunkConsumersVerticles(vertx).onComplete(car -> {
        if (car.succeeded()) {
          handler.handle(Future.succeededFuture(true));
        } else {
          handler.handle(Future.failedFuture(car.cause()));
        }
      });
    } catch (Throwable th) {
      th.printStackTrace();
      handler.handle(Future.failedFuture(th));
    }
  }

  private Future<String> deployParsedMarcChunkConsumersVerticles(Vertx vertx) {
    //TODO: get rid of this workaround with global spring context
    ParsedMarcChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));

    Promise<String> deployConsumers = Promise.promise();

    vertx.deployVerticle("org.folio.verticle.consumers.ParsedMarcChunkConsumersVerticle",
      new DeploymentOptions().setWorker(true).setInstances(parsedMarcChunkConsumerInstancesNumber), deployConsumers);

    return deployConsumers.future();
  }

}
