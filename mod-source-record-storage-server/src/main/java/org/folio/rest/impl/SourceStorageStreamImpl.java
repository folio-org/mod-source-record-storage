package org.folio.rest.impl;

import static io.vertx.core.http.HttpHeaders.CONNECTION;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static org.folio.dao.util.RecordDaoUtil.filterRecordBySnapshotId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.dao.util.RecordDaoUtil.toRecordOrderFields;

import java.util.List;
import java.util.Map;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.folio.dataimport.util.ExceptionHelper;
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
  public void getSourceStorageStreamRecords(String snapshotId, String state, List<String> orderBy,
      @Min(0) @Max(2147483647) int offset, @Min(0) @Max(2147483647) int limit, RoutingContext routingContext,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    HttpServerResponse response = routingContext.response();
    response.setStatusCode(200);
    response.setChunked(true);
    response.putHeader(CONTENT_TYPE, "application/stream+json");
    response.putHeader(CONNECTION, "keep-alive");
    Condition condition = filterRecordBySnapshotId(snapshotId).and(filterRecordByState(state));
    List<OrderField<?>> orderFields = toRecordOrderFields(orderBy);
    Flowable<Buffer> flowable = recordService.streamRecords(condition, orderFields, offset, limit, tenantId)
      .map(Json::encodeToBuffer)
      .map(buffer -> buffer.appendString(StringUtils.LF));
    Pump.pump(FlowableHelper.toReadStream(flowable).endHandler(end -> {
      response.end();
      response.close();
    }), response).start();
    flowable.doOnError(cause -> {
      LOG.error(cause.getMessage(), cause);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(cause)));
    });
  }

}
