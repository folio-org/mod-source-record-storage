package org.folio.rest.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.folio.dao.util.LbRecordDaoUtil;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.resource.LbSourceStorageSourceRecords;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.LbRecordService;
import org.folio.spring.SpringContextUtil;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LbSourceStorageSourceRecordsImpl implements LbSourceStorageSourceRecords {

  private static final Logger LOG = LoggerFactory.getLogger(LbSourceStorageSourceRecordsImpl.class);
  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private LbRecordService recordService;

  private final String tenantId;

  public LbSourceStorageSourceRecordsImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getLbSourceStorageSourceRecords(String instanceId, String recordType, boolean suppressFromDiscovery,
      Date updatedAfter, Date updatedBefore, List<String> orderBy, int offset, int limit,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = LbRecordDaoUtil.conditionFilterBy(instanceId, recordType, suppressFromDiscovery,
          updatedAfter, updatedBefore);
        List<OrderField<?>> orderFields = LbRecordDaoUtil.toOrderFields(orderBy);
        recordService.getSourceRecords(condition, orderFields, offset, limit, tenantId)
          .map(GetLbSourceStorageSourceRecordsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get source records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageSourceRecordsById(String id, String idType, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getSourceRecordById(id, idType, tenantId)
          .map(optionalSourceRecord -> optionalSourceRecord.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, SourceRecord.class.getSimpleName(), id))))
          .map(GetLbSourceStorageSourceRecordsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get source record by id: {}", id, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
  
}