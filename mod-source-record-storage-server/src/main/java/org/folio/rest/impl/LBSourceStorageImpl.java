package org.folio.rest.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.folio.dao.util.LBRecordDaoUtil;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.resource.LbSourceStorage;
import org.folio.rest.jaxrs.resource.SourceStorage.GetSourceStorageSnapshotsResponse;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.LBRecordService;
import org.folio.services.LBSnapshotService;
import org.folio.spring.SpringContextUtil;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LBSourceStorageImpl implements LbSourceStorage {

  private static final Logger LOG = LoggerFactory.getLogger(LBSourceStorageImpl.class);
  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private LBSnapshotService snapshotService;

  @Autowired
  private LBRecordService recordService;

  private final String tenantId;

  public LBSourceStorageImpl(Vertx vertx, String tenantId) { //NOSONAR
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
  public void getLbSourceStorageSnapshots(int offset, int limit, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        snapshotService.getSnapshots(DSL.trueCondition(), new ArrayList<>(), offset, limit, tenantId)
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
          .map(deleted -> deleted ?
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

  @Override
  public void postLbSourceStorageRecords(String lang, Record entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.saveRecord(entity, tenantId)
          .map((Response) PostLbSourceStorageRecordsResponse
            .respond201WithApplicationJson(entity, PostLbSourceStorageRecordsResponse.headersFor201()))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create record", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageRecords(String snapshotId, List<String> orderBy, int offset, int limit, String lang,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = LBRecordDaoUtil.conditionFilterBy(snapshotId);
        List<OrderField<?>> orderFields = LBRecordDaoUtil.toOrderFields(orderBy);
        recordService.getRecords(condition, orderFields, offset, limit, tenantId)
          .map(GetLbSourceStorageRecordsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putLbSourceStorageRecordsById(String id, String lang, Record entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        recordService.updateRecord(entity, tenantId)
          .map(updated -> PutLbSourceStorageRecordsByIdResponse.respond200WithApplicationJson(entity))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update record {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteLbSourceStorageRecordsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordById(id, tenantId)
          .map(recordOptional -> recordOptional.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
          .compose(record -> record.getDeleted()
            ? Future.succeededFuture(true)
            : recordService.updateRecord(record.withState(State.DELETED), tenantId).map(r -> true))
          .map(updated -> DeleteLbSourceStorageRecordsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete record {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageRecordsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordById(id, tenantId)
          .map(optionalRecord -> optionalRecord.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
          .map(GetLbSourceStorageRecordsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get record by id {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageRecordsFormattedById(String id, String idType, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getFormattedRecord(idType, id, tenantId)
          .map(GetLbSourceStorageRecordsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get record by {} id {}", e, idType, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putLbSourceStorageRecordsSuppressFromDiscoveryById(String id, String idType, boolean suppress,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordByExternalId(id, idType, tenantId)
          .map(recordOptional -> recordOptional.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
          .compose(record -> record.getDeleted()
            ? Future.succeededFuture(true)
            : recordService.updateRecord(record.withAdditionalInfo(record.getAdditionalInfo()
                .withSuppressDiscovery(suppress)), tenantId).map(r -> true))
          .map(updated -> DeleteLbSourceStorageRecordsByIdResponse.respond204())
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete record {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getLbSourceStorageSourceRecords(String instanceId, String recordType, boolean suppressFromDiscovery,
      Date updatedAfter, Date updatedBefore, List<String> orderBy, int offset, int limit,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = LBRecordDaoUtil.conditionFilterBy(instanceId, recordType, suppressFromDiscovery,
          updatedAfter, updatedBefore);
        List<OrderField<?>> orderFields = LBRecordDaoUtil.toOrderFields(orderBy);
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