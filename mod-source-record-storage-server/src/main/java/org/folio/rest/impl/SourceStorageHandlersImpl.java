package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.resource.SourceStorageHandlers;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.EventHandlingService;
import org.folio.services.UpdateRecordEventHandler;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageHandlersImpl implements SourceStorageHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageHandlersImpl.class);

  @Autowired
  private EventHandlingService instanceCreatedEventHandleService;
  @Autowired
  private UpdateRecordEventHandler updateRecordEventHandler;

  private String tenantId;

  public SourceStorageHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageHandlersCreatedInventoryInstance(String entity, Map<String, String> okapiHeaders,
                                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOG.debug("Event DI_INVENTORY_INSTANCE_CREATED was received: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(PostSourceStorageHandlersCreatedInventoryInstanceResponse.respond200()));

      // response status doesn't depend on event handling result
      instanceCreatedEventHandleService.handle(entity, tenantId);
    });
  }

  @Override
  public void postSourceStorageHandlersUpdatedRecord(String entity, Map<String, String> okapiHeaders,
                                                     Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    LOG.debug("Received QM_RECORD_UPDATED event: {}", entity);
    asyncResultHandler.handle(Future.succeededFuture(PostSourceStorageHandlersUpdatedRecordResponse.respond204()));

    updateRecordEventHandler.handle(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()));
  }
}
