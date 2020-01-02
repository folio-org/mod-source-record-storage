package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.resource.SourceStorageBatch;
import org.folio.rest.tools.utils.JwtUtils;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TOKEN;
import static org.folio.rest.RestVerticle.OKAPI_USERID_HEADER;

public class SourceStorageBatchImpl implements SourceStorageBatch {
  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageBatchImpl.class);

  @Autowired
  private RecordService recordService;

  private String tenantId;

  public SourceStorageBatchImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStorageBatchRecords(RecordCollection entity, Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        populateRecordsMetaData(entity.getRecords(), okapiHeaders);
        recordService.saveRecords(entity, tenantId)
          .map(recordsBatchResponse -> {
            if (!recordsBatchResponse.getRecords().isEmpty()) {
              return PostSourceStorageBatchRecordsResponse.respond201WithApplicationJson(recordsBatchResponse);
            } else {
              LOG.error("Batch of records was processed, but records were not saved, error messages: {}", recordsBatchResponse.getErrorMessages());
              return PostSourceStorageBatchRecordsResponse.respond500WithApplicationJson(recordsBatchResponse);
            }
          })
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to create records from collection", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putSourceStorageBatchParsedRecords(RecordCollection entity, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        populateRecordsMetaData(entity.getRecords(), okapiHeaders);
        recordService.updateParsedRecords(entity, tenantId)
          .map(parsedRecordsBatchResponse -> {
            if (!parsedRecordsBatchResponse.getParsedRecords().isEmpty()) {
              return PutSourceStorageBatchParsedRecordsResponse.respond200WithApplicationJson(parsedRecordsBatchResponse);
            } else {
              LOG.error("Batch of parsed records was processed, but records were not updated, error messages: {}", parsedRecordsBatchResponse.getErrorMessages());
              return PutSourceStorageBatchParsedRecordsResponse.respond500WithApplicationJson(parsedRecordsBatchResponse);
            }
          })
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to update parsed records", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private void populateRecordsMetaData(List<Record> records, Map<String, String> okapiHeaders) {
    String userId = okapiHeaders.getOrDefault(OKAPI_USERID_HEADER, "");
    String token = okapiHeaders.getOrDefault(OKAPI_HEADER_TOKEN, "");
    if (userId == null && token != null) {
      userId = userIdFromToken(token);
    }
    Metadata md = new Metadata();
    md.setUpdatedDate(new Date());
    md.setUpdatedByUserId(userId);
    md.setCreatedDate(md.getUpdatedDate());
    md.setCreatedByUserId(userId);
    records.forEach(instance -> instance.setMetadata(md));
  }

  private static String userIdFromToken(String token) {
    try {
      String[] split = token.split("\\.");
      String json = JwtUtils.getJson(split[1]);
      JsonObject j = new JsonObject(json);
      return j.getString("user_id");
    } catch (Exception e) {
      LOG.warn("Invalid x-okapi-token: " + token, e);
      return null;
    }
  }

}
