package org.folio.rest.impl;

import static io.vertx.core.http.HttpHeaders.CONNECTION;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import static org.folio.dao.util.RecordDaoUtil.filterRecordByDeleted;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByLeaderRecordStatus;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySuppressFromDiscovery;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByUpdatedDateRange;
import static org.folio.dao.util.RecordDaoUtil.toRecordOrderFields;
import static org.folio.rest.util.QueryParamUtil.firstNonEmpty;
import static org.folio.rest.util.QueryParamUtil.toRecordType;

import java.util.Date;
import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import javax.ws.rs.core.Response;

import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.reactivex.FlowableHelper;
import io.vertx.sqlclient.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;

import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.impl.wrapper.SearchRecordIdsWriteStream;
import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
import org.folio.rest.jaxrs.resource.SourceStorageStream;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordSearchParameters;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;

public class SourceStorageStreamImpl implements SourceStorageStream {

  private static final Logger LOG = LogManager.getLogger();
  private final String tenantId;

  @Autowired
  private RecordService recordService;

  public SourceStorageStreamImpl(Vertx vertx, String tenantId) { // NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getSourceStorageStreamRecords(String snapshotId, String recordType, String state, List<String> orderBy, String totalRecords,
                                            @Min(0) @Max(2147483647) int offset, @Min(0) @Max(2147483647) int limit,
                                            RoutingContext routingContext,
                                            Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    HttpServerResponse response = prepareStreamResponse(routingContext);
    Condition condition = filterRecordBySnapshotId(snapshotId).and(filterRecordByState(state));
    List<OrderField<?>> orderFields = toRecordOrderFields(orderBy, true);
    Flowable<Buffer> flowable = recordService
      .streamRecords(condition, toRecordType(recordType), orderFields, offset, limit, tenantId)
      .map(Json::encodeToBuffer)
      .map(buffer -> buffer.appendString(StringUtils.LF));
    processStream(response, flowable, cause -> {
      LOG.warn(cause.getMessage(), cause);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(cause)));
    });
  }

  @Override
  public void getSourceStorageStreamSourceRecords(String recordId, String snapshotId, String externalId, String externalHrid,
                                                  String instanceId, String instanceHrid, String holdingsId,
                                                  String holdingsHrid, String recordType, Boolean suppressFromDiscovery,
                                                  Boolean deleted, String leaderRecordStatus, Date updatedAfter,
                                                  Date updatedBefore, List<String> orderBy,  String totalRecords, int offset, int limit,
                                                  RoutingContext routingContext, Map<String, String> okapiHeaders,
                                                  Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    HttpServerResponse response = prepareStreamResponse(routingContext);
    Condition condition = filterRecordByRecordId(recordId)
      .and(filterRecordBySnapshotId(snapshotId))
      .and(filterRecordByExternalId(firstNonEmpty(externalId, instanceId, holdingsId)))
      .and(filterRecordByExternalHrid(firstNonEmpty(externalHrid, instanceHrid, holdingsHrid)))
      .and(filterRecordBySuppressFromDiscovery(suppressFromDiscovery))
      .and(filterRecordByDeleted(deleted))
      .and(filterRecordByLeaderRecordStatus(leaderRecordStatus))
      .and(filterRecordByUpdatedDateRange(updatedAfter, updatedBefore));
    List<OrderField<?>> orderFields = toRecordOrderFields(orderBy, true);
    Flowable<Buffer> flowable = recordService
      .streamSourceRecords(condition, toRecordType(recordType), orderFields, offset, limit, tenantId)
      .map(Json::encodeToBuffer)
      .map(buffer -> buffer.appendString(StringUtils.LF));
    processStream(response, flowable, cause -> {
      LOG.warn(cause.getMessage(), cause);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(cause)));
    });
  }

  @Override
  public void postSourceStorageStreamMarcRecordIdentifiers(MarcRecordSearchRequest request, RoutingContext routingContext,
                                                           Map<String, String> okapiHeaders,
                                                           Handler<AsyncResult<Response>> asyncResultHandler,
                                                           Context vertxContext) {
    HttpServerResponse response = prepareStreamResponse(routingContext);
    Flowable<Row> flowable = recordService.streamMarcRecordIds(RecordSearchParameters.from(request), tenantId);
    processStream(new SearchRecordIdsWriteStream(response), flowable, cause -> {
      LOG.warn(cause.getMessage(), cause);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(cause)));
    });
  }

  private void processStream(SearchRecordIdsWriteStream responseWrapper, Flowable<Row> flowable,
                             Handler<Throwable> errorHandler) {
    FlowableHelper.toReadStream(flowable)
      .exceptionHandler(errorHandler)
      .endHandler(end -> responseWrapper.end())
      .pipeTo(responseWrapper);
    flowable.doOnError(errorHandler::handle);
  }

  private void processStream(HttpServerResponse response, Flowable<Buffer> flowable, Handler<Throwable> errorHandler) {
    FlowableHelper.toReadStream(flowable)
      .exceptionHandler(errorHandler)
      .endHandler(end -> response.end())
      .pipeTo(response);
    flowable.doOnError(errorHandler::handle);
  }

  private HttpServerResponse prepareStreamResponse(RoutingContext routingContext) {
    return routingContext.response()
      .setStatusCode(200)
      .setChunked(true)
      .putHeader(CONTENT_TYPE, "application/stream+json")
      .putHeader(CONNECTION, "keep-alive");
  }

}
