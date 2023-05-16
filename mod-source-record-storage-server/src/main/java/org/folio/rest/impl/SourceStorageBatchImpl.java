package org.folio.rest.impl;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.FetchParsedRecordsBatchRequest;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.resource.SourceStorageBatch;
import org.folio.rest.tools.utils.MetadataUtil;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SourceStorageBatchImpl implements SourceStorageBatch {

  private static final Logger LOG = LogManager.getLogger();

  @Autowired
  private RecordService recordService;

  private String tenantId;

  public SourceStorageBatchImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageBatchVerifiedRecords(List<String> marcBibIds, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.verifyMarcBibRecords(marcBibIds, tenantId)
          .map(PostSourceStorageBatchVerifiedRecordsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageBatchVerifiedRecords:: Failed to receive marc bib records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageBatchRecords(RecordCollection entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        MetadataUtil.populateMetadata(entity.getRecords(), okapiHeaders);
        recordService.saveRecords(entity, tenantId)
          .map(recordsBatchResponse -> {
            if (!recordsBatchResponse.getRecords().isEmpty()) {
              return PostSourceStorageBatchRecordsResponse.respond201WithApplicationJson(recordsBatchResponse);
            } else {
              LOG.warn("postSourceStorageBatchRecords:: Batch of records was processed, but records were not saved, error messages: {}", recordsBatchResponse.getErrorMessages());
              return PostSourceStorageBatchRecordsResponse.respond500WithApplicationJson(recordsBatchResponse);
            }
          })
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageBatchRecords:: Failed to create records from collection", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageBatchParsedRecords(RecordCollection entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        MetadataUtil.populateMetadata(entity.getRecords(), okapiHeaders);
        recordService.updateParsedRecords(entity, tenantId)
          .map(parsedRecordsBatchResponse -> {
            if (!parsedRecordsBatchResponse.getParsedRecords().isEmpty()) {
              return PutSourceStorageBatchParsedRecordsResponse.respond200WithApplicationJson(parsedRecordsBatchResponse);
            } else {
              LOG.warn("putSourceStorageBatchParsedRecords:: Batch of parsed records was processed, but records were not updated, error messages: {}", parsedRecordsBatchResponse.getErrorMessages());
              return PutSourceStorageBatchParsedRecordsResponse.respond500WithApplicationJson(parsedRecordsBatchResponse);
            }
          })
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("putSourceStorageBatchParsedRecords:: Failed to update parsed records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageBatchParsedRecordsFetch(FetchParsedRecordsBatchRequest entity,
                                                       Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler,
                                                       Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.fetchStrippedParsedRecords(entity, tenantId)
          .map(PostSourceStorageBatchParsedRecordsFetchResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageBatchParsedRecordsFetch:: Failed to fetch parsed records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
