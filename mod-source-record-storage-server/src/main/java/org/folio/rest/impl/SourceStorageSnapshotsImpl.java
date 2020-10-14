package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.resource.SourceStorageSnapshots;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.SnapshotRemovalService;
import org.folio.services.SnapshotService;
import org.folio.spring.SpringContextUtil;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

import static org.folio.dao.util.SnapshotDaoUtil.filterSnapshotByStatus;
import static org.folio.dao.util.SnapshotDaoUtil.toSnapshotOrderFields;

public class SourceStorageSnapshotsImpl implements SourceStorageSnapshots {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageSnapshotsImpl.class);
  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private SnapshotService snapshotService;
  @Autowired
  private SnapshotRemovalService snapshotRemovalService;

  private final String tenantId;

  public SourceStorageSnapshotsImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageSnapshots(String lang, Snapshot entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.saveSnapshot(entity, tenantId)
          .map((Response) PostSourceStorageSnapshotsResponse
            .respond201WithApplicationJson(entity, PostSourceStorageSnapshotsResponse.headersFor201()))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create snapshot", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageSnapshots(String status, List<String> orderBy, int offset, int limit, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = filterSnapshotByStatus(status);
        List<OrderField<?>> orderFields = toSnapshotOrderFields(orderBy);
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
  public void putSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang, Snapshot entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setJobExecutionId(jobExecutionId);
        snapshotService.updateSnapshot(entity, tenantId)
          .map(updated -> PutSourceStorageSnapshotsByJobExecutionIdResponse.respond200WithApplicationJson(entity))
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
  public void deleteSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotRemovalService.deleteSnapshot(jobExecutionId, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()))
          .map(deleted -> Boolean.TRUE.equals(deleted)
            ? DeleteSourceStorageSnapshotsByJobExecutionIdResponse.respond204()
            : DeleteSourceStorageSnapshotsByJobExecutionIdResponse.respond404WithTextPlain(
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
  public void getSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.getSnapshotById(jobExecutionId, tenantId)
          .map(optionalSnapshot -> optionalSnapshot.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId))))
          .map(GetSourceStorageSnapshotsByJobExecutionIdResponse::respond200WithApplicationJson)
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
