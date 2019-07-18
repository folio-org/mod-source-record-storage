package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordBatch;
import org.folio.rest.jaxrs.resource.SourceStorageBatch;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageBatchImpl implements SourceStorageBatch {
  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageBatchImpl.class);

  @Autowired
  private RecordService recordService;

  private String tenantId;

  public SourceStorageBatchImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageBatchRecordsCreate(RecordBatch entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.saveRecords(entity, tenantId)
          .map((RecordBatch it) -> (Response) PostSourceStorageBatchRecordsCreateResponse.respond207WithApplicationJson(it))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create records from collection", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageBatchParsedRecords(ParsedRecordCollection entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.updateParsedRecords(entity, tenantId)
                .map((ParsedRecordCollection it) -> {
                  if (it.getErrorMessages().isEmpty()) {
                    return (Response) PutSourceStorageBatchParsedRecordsResponse.respond200WithApplicationJson(it);
                  } else {
                    LOG.error("Some records were not saved! Here is the list of errors: {}", it.getErrorMessages());
                    return (Response) PutSourceStorageBatchParsedRecordsResponse.respond500WithApplicationJson(it);
                  }
                })
                .otherwise(ExceptionHelper::mapExceptionToResponse)
                .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update parsed records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
