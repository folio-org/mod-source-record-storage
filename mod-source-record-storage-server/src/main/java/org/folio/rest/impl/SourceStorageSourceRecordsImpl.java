package org.folio.rest.impl;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByLeaderRecordState;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySuppressFromDiscovery;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByType;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByUpdatedDateRange;
import static org.folio.dao.util.RecordDaoUtil.toRecordOrderFields;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.resource.SourceStorageSourceRecords;
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

public class SourceStorageSourceRecordsImpl implements SourceStorageSourceRecords {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageSourceRecordsImpl.class);

  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";

  @Autowired
  private RecordService recordService;

  private final String tenantId;

  public SourceStorageSourceRecordsImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getSourceStorageSourceRecords(String recordId, String snapshotId, String instanceId, String recordType,
      String recordState, Boolean suppressFromDiscovery, String leaderRecordStatus, Date updatedAfter, Date updatedBefore,
      List<String> orderBy, int offset, int limit, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    // NOTE: if and when a new record type is introduced and a parsed record table is added,
    // will need to add a record type query parameter
    vertxContext.runOnContext(v -> {
      try {
        Condition condition = filterRecordByRecordId(recordId)
          .and(filterRecordBySnapshotId(snapshotId))
          .and(filterRecordByInstanceId(instanceId))
          .and(filterRecordByType(recordType))
          .and(filterRecordByState(recordState))
          .and(filterRecordBySuppressFromDiscovery(suppressFromDiscovery))
          .and(filterRecordByLeaderRecordState(leaderRecordStatus))
          .and(filterRecordByUpdatedDateRange(updatedAfter, updatedBefore));
        List<OrderField<?>> orderFields = toRecordOrderFields(orderBy);
        recordService.getSourceRecords(condition, orderFields, offset, limit, tenantId)
          .map(GetSourceStorageSourceRecordsResponse::respond200WithApplicationJson)
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
  public void getSourceStorageSourceRecordsById(String id, String idType, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        recordService.getSourceRecordById(id, idType, tenantId)
          .map(optionalSourceRecord -> optionalSourceRecord.orElseThrow(() ->
            new NotFoundException(String.format(NOT_FOUND_MESSAGE, SourceRecord.class.getSimpleName(), id))))
          .map(GetSourceStorageSourceRecordsByIdResponse::respond200WithApplicationJson)
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