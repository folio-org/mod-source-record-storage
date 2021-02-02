package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.resource.SourceStorageHandlers;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.UpdateRecordEventHandlingService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageHandlersImpl implements SourceStorageHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageHandlersImpl.class);

  @Autowired
  private UpdateRecordEventHandlingService updateRecordEventHandlingService;

  public SourceStorageHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postSourceStorageHandlersUpdatedRecord(String entity, Map<String, String> okapiHeaders,
                                                     Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOG.debug("Received QM_RECORD_UPDATED event: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(PostSourceStorageHandlersUpdatedRecordResponse.respond204()));
      // response status doesn't depend on event handling result
      updateRecordEventHandlingService.handleEvent(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()));
    });
  }
}
