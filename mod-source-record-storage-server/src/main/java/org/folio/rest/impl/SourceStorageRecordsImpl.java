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
import org.folio.rest.jaxrs.model.RecordMatchingDto;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SourceStorageRecordsImpl implements SourceStorageRecords {

  private static final Logger LOG = LogManager.getLogger();

  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private RecordService recordService;

  private final String tenantId;

  public SourceStorageRecordsImpl(Vertx vertx, String tenantId) { // NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageRecords(Record entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.saveRecord(entity, okapiHeaders)
          .map((Response) PostSourceStorageRecordsResponse.respond201WithApplicationJson(entity, PostSourceStorageRecordsResponse.headersFor201()))
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageRecords:: Failed to create record", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecords(String snapshotId, String recordType, String state, List<String> orderBy,  String totalRecords, int offset, int limit,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
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
        LOG.warn("getSourceStorageRecords:: Failed to get all records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageRecordsById(String id, Record entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(id);
        recordService.updateRecord(entity, okapiHeaders)
          .map(updated -> PutSourceStorageRecordsByIdResponse.respond200WithApplicationJson(entity))
          .map(Response.class::cast).otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("putSourceStorageRecordsById:: Failed to update record by id {}", id, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
  @Override
  public void putSourceStorageRecordsGenerationById(String matchedId, Record entity, Map<String, String> okapiHeaders,
                                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.updateRecordGeneration(matchedId, entity, okapiHeaders)
          .map(updated -> PutSourceStorageRecordsGenerationByIdResponse.respond200WithApplicationJson(entity))
          .map(Response.class::cast).otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("putSourceStorageRecordsGenerationById:: Failed to update record generation by matchedId {}", matchedId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteSourceStorageRecordsById(String id, String idType, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.deleteRecordById(id, toExternalIdType(idType), okapiHeaders).map(r -> true)
            .map(updated -> DeleteSourceStorageRecordsByIdResponse.respond204()).map(Response.class::cast)
            .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("deleteSourceStorageRecordsById:: Failed to delete record by id {}", id, e );
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageRecordsById(String id, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getRecordById(id, tenantId)
          .map(optionalRecord -> optionalRecord.orElseThrow(() -> new NotFoundException(format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
          .map(GetSourceStorageRecordsByIdResponse::respond200WithApplicationJson).map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("getSourceStorageRecordsById:: Failed to get record by id {}", id, e);
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
        LOG.warn("getSourceStorageRecordsFormattedById:: Failed to get record by {} id {}", idType, id, e);
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
        LOG.warn("putSourceStorageRecordsSuppressFromDiscoveryById:: Failed to update record's SuppressFromDiscovery flag", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageRecordsMatching(RecordMatchingDto recordMatchingDto, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getMatchedRecordsIdentifiers(recordMatchingDto, tenantId)
          .map(PostSourceStorageRecordsMatchingResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageRecordsMatching:: Failed to get identifiers of records by matching criteria", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageRecordsUnDeleteById(String id, String idType, Map<String, String> okapiHeaders,
                                                   Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.unDeleteRecordById(id, toExternalIdType(idType), okapiHeaders).map(r -> true)
          .map(updated -> PostSourceStorageRecordsUnDeleteByIdResponse.respond204()).map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse).onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageRecordsUnDeleteById:: Failed to undelete record by id {}", id, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
