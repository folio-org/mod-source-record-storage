package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.resource.SourceStorageHandlers;

import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageHandlersImpl implements SourceStorageHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageHandlersImpl.class);

  public SourceStorageHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
  }

  @Override
  public void postSourceStorageHandlersCreatedInventoryInstance(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        // endpoint will be implemented in https://issues.folio.org/browse/MODSOURCE-96
        LOG.info("Event was received: {}", entity);
        Future.succeededFuture((Response) SourceStorageHandlers.PostSourceStorageHandlersCreatedInventoryInstanceResponse.respond200())
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to handle event", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
