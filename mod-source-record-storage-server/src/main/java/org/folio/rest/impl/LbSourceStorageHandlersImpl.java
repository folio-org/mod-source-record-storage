package org.folio.rest.impl;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.resource.LbSourceStorageHandlers;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.LbInstanceEventHandlingService;
import org.folio.services.LbUpdateRecordEventHandlingService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LbSourceStorageHandlersImpl implements LbSourceStorageHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(LbSourceStorageHandlersImpl.class);

  @Autowired
  private LbInstanceEventHandlingService instanceEventHandlingService;

  @Autowired
  private LbUpdateRecordEventHandlingService updateRecordEventHandlingService;

  public LbSourceStorageHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postLbSourceStorageHandlersInventoryInstance(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOG.debug("Event DI_INVENTORY_INSTANCE_CREATED or DI_INVENTORY_INSTANCE_UPDATED was received: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(PostLbSourceStorageHandlersInventoryInstanceResponse.respond200()));
      // response status doesn't depend on event handling result
      instanceEventHandlingService.handleEvent(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()));
    });
  }

  @Override
  public void postLbSourceStorageHandlersUpdatedRecord(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOG.debug("Received QM_RECORD_UPDATED event: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(PostLbSourceStorageHandlersUpdatedRecordResponse.respond204()));
      // response status doesn't depend on event handling result
      updateRecordEventHandlingService.handleEvent(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()));
    });
  }
}
