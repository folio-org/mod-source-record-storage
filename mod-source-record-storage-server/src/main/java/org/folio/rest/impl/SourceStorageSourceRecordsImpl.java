package org.folio.rest.impl;

import static java.lang.String.format;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByDeleted;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByLeaderRecordStatus;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySuppressFromDiscovery;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByUpdatedDateRange;
import static org.folio.dao.util.RecordDaoUtil.toRecordOrderFields;
import static org.folio.rest.util.QueryParamUtil.firstNonEmpty;
import static org.folio.rest.util.QueryParamUtil.toExternalIdType;
import static org.folio.rest.util.QueryParamUtil.toRecordType;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.resource.SourceStorageSourceRecords;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;

public class SourceStorageSourceRecordsImpl implements SourceStorageSourceRecords {

  private static final Logger LOG = LogManager.getLogger();

  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private RecordService recordService;

  private final String tenantId;

  public SourceStorageSourceRecordsImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getSourceStorageSourceRecords(String recordId, String snapshotId, String externalId, String externalHrid,
                                            String instanceId, String instanceHrid, String holdingsId, String holdingsHrid,
                                            String recordType, Boolean suppressFromDiscovery, Boolean deleted,
                                            String leaderRecordStatus, Date updatedAfter, Date updatedBefore,
                                            List<String> orderBy, int offset, int limit, Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = filterRecordByRecordId(recordId)
          .and(filterRecordBySnapshotId(snapshotId))
          .and(filterRecordByExternalId(firstNonEmpty(externalId, instanceId, holdingsId)))
          .and(filterRecordByExternalHrid(firstNonEmpty(externalHrid, instanceHrid, holdingsHrid)))
          .and(filterRecordBySuppressFromDiscovery(suppressFromDiscovery))
          .and(filterRecordByDeleted(deleted))
          .and(filterRecordByLeaderRecordStatus(leaderRecordStatus))
          .and(filterRecordByUpdatedDateRange(updatedAfter, updatedBefore));

        boolean forOffset = offset != 0 || limit != 1;
        List<OrderField<?>> orderFields = toRecordOrderFields(orderBy, forOffset);
        recordService.getSourceRecords(condition, toRecordType(recordType), orderFields, offset, limit, tenantId)
          .map(GetSourceStorageSourceRecordsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("getSourceStorageSourceRecords:: Failed to get source records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postSourceStorageSourceRecords(String idType, String recordType, Boolean deleted, List<String> entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getSourceRecords(entity, toExternalIdType(idType), toRecordType(recordType), deleted, tenantId)
          .map(GetSourceStorageSourceRecordsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("postSourceStorageSourceRecords:: Failed to get source records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getSourceStorageSourceRecordsById(String id, String idType, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getSourceRecordById(id, toExternalIdType(idType), tenantId)
          .map(optionalSourceRecord -> optionalSourceRecord.orElseThrow(() ->
            new NotFoundException(format(NOT_FOUND_MESSAGE, SourceRecord.class.getSimpleName(), id))))
          .map(GetSourceStorageSourceRecordsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOG.warn("getSourceStorageSourceRecordsById:: Failed to get source record by id: {}", id, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

}
