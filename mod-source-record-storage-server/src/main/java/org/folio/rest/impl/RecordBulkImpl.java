package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.folio.rest.jaxrs.resource.RecordBulk;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.BulkRecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class RecordBulkImpl implements RecordBulk {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageBatchImpl.class);

  @Autowired
  private BulkRecordService bulkRecordService;

  public RecordBulkImpl(Vertx vertx, String tenantId) {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postRecordBulk(RoutingContext routingContext, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
    bulkRecordService.dummySearchRecords(routingContext, asyncResultHandler);
  }
}
