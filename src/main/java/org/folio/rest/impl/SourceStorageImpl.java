package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.jaxrs.resource.SourceStorage;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.SnapshotService;
import org.folio.services.SnapshotServiceImpl;
import org.folio.util.SourceStorageHelper;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageImpl implements SourceStorage {

  private static final Logger LOG = LoggerFactory.getLogger("mod-source-record-storage");
  private SnapshotService snapshotService;

  public SourceStorageImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.snapshotService = new SnapshotServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void getSourceStorageSnapshot(String query, int offset, int limit, String lang,
                                       Map<String, String> okapiHeaders,
                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.getSnapshots(query, offset, limit)
          .map(snapshots -> new SnapshotCollection()
            .withSnapshots(snapshots)
            .withTotalRecords(snapshots.size())
          ).map(GetSourceStorageSnapshotResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all snapshots", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageSnapshot(String lang, Snapshot entity, Map<String, String> okapiHeaders,
                                        Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.saveSnapshot(entity)
          .map((Response) PostSourceStorageSnapshotResponse
            .respond201WithApplicationJson(entity, PostSourceStorageSnapshotResponse.headersFor201()))
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create a snapshot", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageSnapshotByJobExecutionId(String jobExecutionId, Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler,
                                                       Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.getSnapshotById(jobExecutionId)
          .map(optionalSnapshot -> optionalSnapshot.orElseThrow(() ->
            new NotFoundException(String.format("Snapshot with id '%s' was not found", jobExecutionId))))
          .map(GetSourceStorageSnapshotByJobExecutionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get snapshot by jobExecutionId", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageSnapshotByJobExecutionId(String jobExecutionId, Snapshot entity,
                                                       Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler,
                                                       Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setJobExecutionId(jobExecutionId);
        snapshotService.updateSnapshot(entity)
          .map(updated -> updated ?
            PutSourceStorageSnapshotByJobExecutionIdResponse.respond200WithApplicationJson(entity) :
            PutSourceStorageSnapshotByJobExecutionIdResponse.respond404WithTextPlain(
              String.format("Snapshot with id '%s' was not found", jobExecutionId))
          )
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update a snapshot", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteSourceStorageSnapshotByJobExecutionId(String jobExecutionId, Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.deleteSnapshot(jobExecutionId)
          .map(deleted -> deleted ?
            DeleteSourceStorageSnapshotByJobExecutionIdResponse.respond204WithTextPlain(
              String.format("Snapshot with id '%s' was successfully deleted", jobExecutionId)) :
            DeleteSourceStorageSnapshotByJobExecutionIdResponse.respond404WithTextPlain(
              String.format("Snapshot with id '%s' was not found", jobExecutionId)))
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete a snapshot", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
