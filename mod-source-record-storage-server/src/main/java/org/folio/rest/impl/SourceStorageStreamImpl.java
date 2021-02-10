package org.folio.rest.impl;

import static io.vertx.core.http.HttpHeaders.CONNECTION;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByDeleted;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByInstanceId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByLeaderRecordStatus;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySuppressFromDiscovery;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByUpdatedDateRange;
import static org.folio.dao.util.RecordDaoUtil.toRecordOrderFields;
import static org.folio.rest.util.QueryParamUtil.toRecordType;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Pattern;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.RecordsSearchRequest;
import org.folio.rest.jaxrs.resource.SourceStorageStream;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;

import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.streams.Pump;
import io.vertx.ext.web.RoutingContext;
import io.vertx.reactivex.FlowableHelper;

public class SourceStorageStreamImpl implements SourceStorageStream {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageStreamImpl.class);

  @Autowired
  private RecordService recordService;

  private final String tenantId;

  public SourceStorageStreamImpl(Vertx vertx, String tenantId) { // NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getSourceStorageStreamRecords(String snapshotId, String recordType, String state, List<String> orderBy,
      @Min(0) @Max(2147483647) int offset, @Min(0) @Max(2147483647) int limit, RoutingContext routingContext,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    HttpServerResponse response = prepareStreamResponse(routingContext);
    Condition condition = filterRecordBySnapshotId(snapshotId).and(filterRecordByState(state));
    List<OrderField<?>> orderFields = toRecordOrderFields(orderBy, true);
    Flowable<Buffer> flowable = recordService.streamRecords(condition, toRecordType(recordType), orderFields, offset, limit, tenantId)
      .map(Json::encodeToBuffer)
      .map(buffer -> buffer.appendString(StringUtils.LF));
    processStream(response, flowable, cause -> {
      LOG.error(cause.getMessage(), cause);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(cause)));
    });
  }

  @Override
  public void getSourceStorageStreamSourceRecords(String recordId, String snapshotId, String instanceId,
      String instanceHrid, String recordType, Boolean suppressFromDiscovery, Boolean deleted,
      @Pattern(regexp = "^[a|c|d|n|p|o|s|x]{1}$") String leaderRecordStatus, Date updatedAfter, Date updatedBefore,
      List<String> orderBy, @Min(0) @Max(2147483647) int offset, @Min(0) @Max(2147483647) int limit,
      RoutingContext routingContext, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    HttpServerResponse response = prepareStreamResponse(routingContext);
    Condition condition = filterRecordByRecordId(recordId)
      .and(filterRecordBySnapshotId(snapshotId))
      .and(filterRecordByInstanceId(instanceId))
      .and(filterRecordByInstanceHrid(instanceHrid))
      .and(filterRecordBySuppressFromDiscovery(suppressFromDiscovery))
      .and(filterRecordByDeleted(deleted))
      .and(filterRecordByLeaderRecordStatus(leaderRecordStatus))
      .and(filterRecordByUpdatedDateRange(updatedAfter, updatedBefore));
    List<OrderField<?>> orderFields = toRecordOrderFields(orderBy, true);
    Flowable<Buffer> flowable = recordService.streamSourceRecords(condition, toRecordType(recordType), orderFields, offset, limit, tenantId)
      .map(Json::encodeToBuffer)
      .map(buffer -> buffer.appendString(StringUtils.LF));
    processStream(response, flowable, cause -> {
      LOG.error(cause.getMessage(), cause);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(cause)));
    });
  }

  @Override
  public void postSourceStorageStreamSourceRecords(RecordsSearchRequest entity, RoutingContext routingContext, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncResultHandler.handle(Future.succeededFuture());
  }

  private void processStream(HttpServerResponse response, Flowable<Buffer> flowable, Handler<Throwable> errorHandler) {
    Pump.pump(FlowableHelper.toReadStream(flowable)
      .exceptionHandler(errorHandler)
      .endHandler(end -> {
        response.end();
        response.close();
      }), response)
      .start();
    flowable.doOnError(cause -> {
      errorHandler.handle(cause);
    });
  }

  private HttpServerResponse prepareStreamResponse(RoutingContext routingContext) {
    return routingContext.response()
      .setStatusCode(200)
      .setChunked(true)
      .putHeader(CONTENT_TYPE, "application/stream+json")
      .putHeader(CONNECTION, "keep-alive");
  }

}
