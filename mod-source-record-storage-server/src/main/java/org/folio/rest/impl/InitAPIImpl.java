package org.folio.rest.impl;

import org.folio.config.ApplicationConfig;
import org.folio.processing.events.EventManager;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.handlers.InstancePostProcessingEventHandler;
import org.folio.services.handlers.MarcBibliographicMatchEventHandler;
import org.folio.services.handlers.actions.ModifyRecordEventHandler;
import org.folio.spring.SpringContextUtil;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;

public class InitAPIImpl implements InitAPI {

  @Autowired
  private InstancePostProcessingEventHandler instancePostProcessingEventHandler;

  @Autowired
  private ModifyRecordEventHandler modifyRecordEventHandler;

  @Autowired
  private MarcBibliographicMatchEventHandler marcBibliographicMatchEventHandler;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    vertx.executeBlocking(
      future -> {
        SpringContextUtil.init(vertx, context, ApplicationConfig.class);
        SpringContextUtil.autowireDependencies(this, context);
        registerEventHandlers();
        future.complete();
      },
      result -> {
        if (result.succeeded()) {
          handler.handle(Future.succeededFuture(true));
        } else {
          handler.handle(Future.failedFuture(result.cause()));
        }
      });
  }

  private void registerEventHandlers() {
    EventManager.registerEventHandler(instancePostProcessingEventHandler);
    EventManager.registerEventHandler(modifyRecordEventHandler);
    EventManager.registerEventHandler(marcBibliographicMatchEventHandler);
  }
}
