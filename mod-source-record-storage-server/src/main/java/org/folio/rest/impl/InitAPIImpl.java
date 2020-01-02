package org.folio.rest.impl;

import com.fasterxml.jackson.databind.JsonDeserializer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.config.ApplicationConfig;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class InitAPIImpl implements InitAPI {

  @Autowired
  private JsonDeserializer<Snapshot> snapshotDeserializer;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    vertx.executeBlocking(
      future -> {
        SpringContextUtil.init(vertx, context, ApplicationConfig.class);
        SpringContextUtil.autowireDependencies(this, context);
        ObjectMapperTool.registerDeserializer(Snapshot.class, snapshotDeserializer);
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
}
