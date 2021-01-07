package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.SearchRecordRqBody;
import org.folio.rest.jaxrs.resource.SourceStorageRecordBulk;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.BulkRecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class RecordBulkImpl implements SourceStorageRecordBulk {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageBatchImpl.class);

  @Autowired
  private BulkRecordService bulkRecordService;

  private String tenantId;

  public RecordBulkImpl(Vertx vertx, String tenantId) {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageRecordBulk(SearchRecordRqBody searchRecordRqBody, RoutingContext routingContext, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      OkapiConnectionParams okapiParams = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
      HttpServerResponse response = routingContext.response();
      prepareResponseForSearch(response);
      bulkRecordService.searchRecords(response, searchRecordRqBody, okapiParams);
    } catch (Exception exception) {
      LOG.error(exception.getMessage(), exception);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(exception)));
    }
  }

  private void prepareResponseForSearch(HttpServerResponse response) {
    response.setStatusCode(200);
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/json");
  }
}
