package org.folio.rest.impl;

import java.util.List;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.folio.dao.util.LbSnapshotDaoUtil;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.resource.LbSourceStorageSnapshots;
import org.folio.rest.jaxrs.resource.SourceStorage.GetSourceStorageSnapshotsResponse;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.LbSnapshotService;
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

public class LbSourceStorageSnapshotsImpl implements LbSourceStorageSnapshots {

  private static final Logger LOG = LoggerFactory.getLogger(LbSourceStorageSnapshotsImpl.class);
  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private LbSnapshotService snapshotService;

  private final String tenantId;

  public LbSourceStorageSnapshotsImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postLbSourceStorageSnapshots(String lang, Snapshot entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.saveSnapshot(entity, tenantId)
          .map((Response) PostLbSourceStorageSnapshotsResponse
            .respond201WithApplicationJson(entity, PostLbSourceStorageSnapshotsResponse.headersFor201()))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create snapshot", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageSnapshots(String status, List<String> orderBy, int offset, int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = LbSnapshotDaoUtil.conditionFilterBy(status);
        List<OrderField<?>> orderFields = LbSnapshotDaoUtil.toOrderFields(orderBy);
        snapshotService.getSnapshots(condition, orderFields, offset, limit, tenantId)
          .map(GetSourceStorageSnapshotsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all snapshots", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putLbSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang, Snapshot entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setJobExecutionId(jobExecutionId);
        snapshotService.updateSnapshot(entity, tenantId)
          .map(updated -> PutLbSourceStorageSnapshotsByJobExecutionIdResponse.respond200WithApplicationJson(entity))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update snapshot {}", e, jobExecutionId);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteLbSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.deleteSnapshot(jobExecutionId, tenantId)
          .map(deleted -> Boolean.TRUE.equals(deleted) ?
            DeleteLbSourceStorageSnapshotsByJobExecutionIdResponse.respond204() :
            DeleteLbSourceStorageSnapshotsByJobExecutionIdResponse.respond404WithTextPlain(
              String.format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId)))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete snapshot {}", e, jobExecutionId);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.getSnapshotById(jobExecutionId, tenantId)
          .map(optionalSnapshot -> optionalSnapshot.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId))))
          .map(GetLbSourceStorageSnapshotsByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get snapshot by jobExecutionId {}", e, jobExecutionId);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
 
}