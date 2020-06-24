package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.resource.SourceStorageHandlers;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.InstanceEventHandlingService;
import org.folio.services.UpdateRecordEventHandlingService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class SourceStorageHandlersImpl implements SourceStorageHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageHandlersImpl.class);

  @Autowired
  private InstanceEventHandlingService instanceEventHandlingService;

  @Autowired
  private UpdateRecordEventHandlingService updateRecordEventHandlingService;

  public SourceStorageHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postSourceStorageHandlersInventoryInstance(String entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOG.debug("Event DI_INVENTORY_INSTANCE_CREATED or DI_INVENTORY_INSTANCE_UPDATED was received: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(PostSourceStorageHandlersInventoryInstanceResponse.respond200()));
      // response status doesn't depend on event handling result
      instanceEventHandlingService.handleEvent(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()));
    });
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
