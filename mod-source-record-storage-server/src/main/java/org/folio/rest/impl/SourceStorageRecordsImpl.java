package org.folio.rest.impl;

import static java.lang.String.format;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.dao.util.RecordDaoUtil.toRecordOrderFields;
import static org.folio.rest.util.QueryParamUtil.toExternalIdType;
import static org.folio.rest.util.QueryParamUtil.toRecordType;

import java.util.List;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.resource.SourceStorageRecords;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
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

public class SourceStorageRecordsImpl implements SourceStorageRecords {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageRecordsImpl.class);

  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private RecordService recordService;

  private final String tenantId;

  public SourceStorageRecordsImpl(Vertx vertx, String tenantId) { // NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageRecords(String lang, Record entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.saveRecord(entity, tenantId)
          .map((Response) PostSourceStorageRecordsResponse.respond201WithApplicationJson(entity, PostSourceStorageRecordsResponse.headersFor201()))
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create record", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecords(String snapshotId, String recordType, String state, List<String> orderBy, int offset, int limit,
      String lang, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = filterRecordBySnapshotId(snapshotId)
          .and(filterRecordByState(state));
        List<OrderField<?>> orderFields = toRecordOrderFields(orderBy, true);
        recordService.getRecords(condition, toRecordType(recordType), orderFields, offset, limit, tenantId)
          .map(GetSourceStorageRecordsResponse::respond200WithApplicationJson).map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageRecordsById(String id, String lang, Record entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        recordService.updateRecord(entity, tenantId)
          .map(updated -> PutSourceStorageRecordsByIdResponse.respond200WithApplicationJson(entity))
          .map(Response.class::cast).otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update record {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteSourceStorageRecordsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordById(id, tenantId)
          .map(recordOptional -> recordOptional.orElseThrow(() -> new NotFoundException(format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
            .compose(record -> record.getState().equals(State.DELETED) ? Future.succeededFuture(true)
              : recordService.updateRecord(record.withState(State.DELETED), tenantId).map(r -> true))
            .map(updated -> DeleteSourceStorageRecordsByIdResponse.respond204()).map(Response.class::cast)
            .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete record {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecordsById(String id, String lang, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordById(id, tenantId)
          .map(optionalRecord -> optionalRecord.orElseThrow(() -> new NotFoundException(format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
          .map(GetSourceStorageRecordsByIdResponse::respond200WithApplicationJson).map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get record by id {}", e, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecordsFormattedById(String id, String idType, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getFormattedRecord(id, toExternalIdType(idType), tenantId)
          .map(GetSourceStorageRecordsByIdResponse::respond200WithApplicationJson).map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get record by {} id {}", e, idType, id);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageRecordsSuppressFromDiscoveryById(String id, String idType, boolean suppress,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.updateSuppressFromDiscoveryForRecord(id, toExternalIdType(idType), suppress, tenantId)
          .map(PutSourceStorageRecordsSuppressFromDiscoveryByIdResponse::respond200WithTextPlain)
          .map(Response.class::cast).otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update record's SuppressFromDiscovery flag", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

}
