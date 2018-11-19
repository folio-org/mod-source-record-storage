package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.ResultCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.jaxrs.resource.SourceStorage;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.services.RecordServiceImpl;
import org.folio.services.SnapshotService;
import org.folio.services.SnapshotServiceImpl;
import org.folio.util.SourceStorageHelper;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.util.Map;

public class SourceStorageImpl implements SourceStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageImpl.class);
  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";
  private SnapshotService snapshotService;
  private RecordService recordService;

  public SourceStorageImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.snapshotService = new SnapshotServiceImpl(vertx, calculatedTenantId);
    this.recordService = new RecordServiceImpl(vertx, calculatedTenantId);
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
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId))))
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
              String.format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId))
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
              String.format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId)))
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete a snapshot", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecord(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
                                     Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecords(query, offset, limit)
          .map(records -> new RecordCollection()
            .withRecords(records)
            .withTotalRecords(records.size())
          ).map(GetSourceStorageRecordResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all records", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageRecord(String lang, Record entity, Map<String, String> okapiHeaders,
                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.saveRecord(entity)
          .map((Response) PostSourceStorageRecordResponse
            .respond201WithApplicationJson(entity, PostSourceStorageRecordResponse.headersFor201()))
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create a record", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecordById(String id, Map<String, String> okapiHeaders,
                                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordById(id)
          .map(optionalRecord -> optionalRecord.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
          .map(GetSourceStorageRecordByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get record by id", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageRecordById(String id, Record entity, Map<String, String> okapiHeaders,
                                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        recordService.updateRecord(entity)
          .map(updated -> updated ?
            PutSourceStorageRecordByIdResponse.respond200WithApplicationJson(entity) :
            PutSourceStorageRecordByIdResponse.respond404WithTextPlain(
              String.format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))
          )
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update a record", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteSourceStorageRecordById(String id, Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.deleteRecord(id)
          .map(deleted -> deleted ?
            DeleteSourceStorageRecordByIdResponse.respond204WithTextPlain(
              String.format("Record with id '%s' was successfully deleted", id)) :
            DeleteSourceStorageRecordByIdResponse.respond404WithTextPlain(
              String.format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id)))
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete a record", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageResult(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                     Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getResults(query, offset, limit)
          .map(results -> new ResultCollection()
            .withResults(results)
            .withTotalRecords(results.size())
          ).map(GetSourceStorageResultResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(SourceStorageHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get results", e);
        asyncResultHandler.handle(Future.succeededFuture(SourceStorageHelper.mapExceptionToResponse(e)));
      }
    });
  }

}
