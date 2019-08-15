package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceStorageFormattedRecordsIdGetIdentifier;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jaxrs.model.TestMarcRecordsCollection;
import org.folio.rest.jaxrs.resource.SourceStorage;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.services.SnapshotService;
import org.folio.spring.SpringContextUtil;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.folio.rest.impl.ModTenantAPI.LOAD_SAMPLE_PARAMETER;

public class SourceStorageImpl implements SourceStorage {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStorageImpl.class);
  private static final String NOT_FOUND_MESSAGE = "%s with id '%s' was not found";
  private static final String STUB_SNAPSHOT_ID = "00000000-0000-0000-0000-000000000000";

  @Autowired
  private SnapshotService snapshotService;
  @Autowired
  private RecordService recordService;

  private String tenantId;

  public SourceStorageImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void getSourceStorageSnapshots(String query, int offset, int limit, String lang,
                                        Map<String, String> okapiHeaders,
                                        Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      snapshotService.getSnapshots(query, offset, limit, tenantId)
        .map(GetSourceStorageSnapshotsResponse::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to get all snapshots", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void postSourceStorageSnapshots(String lang, Snapshot entity, Map<String, String> okapiHeaders,
                                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      snapshotService.saveSnapshot(entity, tenantId)
        .map((Response) PostSourceStorageSnapshotsResponse
          .respond201WithApplicationJson(entity, PostSourceStorageSnapshotsResponse.headersFor201()))
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to create snapshot", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang, Map<String, String> okapiHeaders,
                                                        Handler<AsyncResult<Response>> asyncResultHandler,
                                                        Context vertxContext) {
    try {
      snapshotService.getSnapshotById(jobExecutionId, tenantId)
        .map(optionalSnapshot -> optionalSnapshot.orElseThrow(() ->
          new NotFoundException(format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId))))
        .map(GetSourceStorageSnapshotsByJobExecutionIdResponse::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to get snapshot by jobExecutionId {}", e, jobExecutionId);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void deleteSourceStorageSnapshotsRecordsByJobExecutionId(String jobExecutionId, Map<String, String> okapiHeaders,
                                                                  Handler<AsyncResult<Response>> asyncResultHandler,
                                                                  Context vertxContext) {
    try {
      recordService.deleteRecordsBySnapshotId(jobExecutionId, tenantId)
        .map(deleted -> DeleteSourceStorageSnapshotsRecordsByJobExecutionIdResponse.respond204WithTextPlain(
          format("Successfully deleted records for JobExecution %s", jobExecutionId)))
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to delete records by jobExecutionId {}", e, jobExecutionId);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void putSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang, Snapshot entity,
                                                        Map<String, String> okapiHeaders,
                                                        Handler<AsyncResult<Response>> asyncResultHandler,
                                                        Context vertxContext) {
    try {
      entity.setJobExecutionId(jobExecutionId);
      snapshotService.updateSnapshot(entity, tenantId)
        .map(updated -> PutSourceStorageSnapshotsByJobExecutionIdResponse.respond200WithApplicationJson(entity))
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to update snapshot {}", e, jobExecutionId);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void deleteSourceStorageSnapshotsByJobExecutionId(String jobExecutionId, String lang, Map<String, String> okapiHeaders,
                                                           Handler<AsyncResult<Response>> asyncResultHandler,
                                                           Context vertxContext) {
    try {
      snapshotService.deleteSnapshot(jobExecutionId, tenantId)
        .map(deleted -> deleted ?
          DeleteSourceStorageSnapshotsByJobExecutionIdResponse.respond204WithTextPlain(
            format("Snapshot with id '%s' was successfully deleted", jobExecutionId)) :
          DeleteSourceStorageSnapshotsByJobExecutionIdResponse.respond404WithTextPlain(
            format(NOT_FOUND_MESSAGE, Snapshot.class.getSimpleName(), jobExecutionId)))
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to delete snapshot {}", e, jobExecutionId);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getSourceStorageRecords(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
                                      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      recordService.getRecords(query, offset, limit, tenantId)
        .map(GetSourceStorageRecordsResponse::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to get all records", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void postSourceStorageRecords(String lang, Record entity, Map<String, String> okapiHeaders,
                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      recordService.saveRecord(entity, tenantId)
        .map((Response) PostSourceStorageRecordsResponse
          .respond201WithApplicationJson(entity, PostSourceStorageRecordsResponse.headersFor201()))
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to create record", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getSourceStorageRecordsById(String id, String lang, Map<String, String> okapiHeaders,
                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      recordService.getRecordById(id, tenantId)
        .map(optionalRecord -> optionalRecord.orElseThrow(() ->
          new NotFoundException(format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
        .map(GetSourceStorageRecordsByIdResponse::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to get record by id {}", e, id);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void putSourceStorageRecordSuppressFromDiscovery(SuppressFromDiscoveryDto entity,
                                                          Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    try {
      recordService.updateSuppressFromDiscoveryForRecord(entity, tenantId)
        .map(PutSourceStorageRecordSuppressFromDiscoveryResponse::respond200WithTextPlain)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to update record's SuppressFromDiscovery flag", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void putSourceStorageRecordsById(String id, String lang, Record entity, Map<String, String> okapiHeaders,
                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      entity.setId(id);
      recordService.updateRecord(entity, tenantId)
        .map(updated -> PutSourceStorageRecordsByIdResponse.respond200WithApplicationJson(entity))
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to update record {}", e, id);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void deleteSourceStorageRecordsById(String id, String lang, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      recordService.getRecordById(id, tenantId)
        .map(recordOptional -> recordOptional.orElseThrow(() ->
          new NotFoundException(format(NOT_FOUND_MESSAGE, Record.class.getSimpleName(), id))))
        .compose(record -> record.getDeleted()
          ? Future.succeededFuture(true)
          : recordService.updateRecord(record.withDeleted(true), tenantId).map(r -> true))
        .map(updated -> DeleteSourceStorageRecordsByIdResponse.respond204WithTextPlain(
          format("Record with id '%s' was successfully deleted", id)))
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to delete record {}", e, id);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getSourceStorageSourceRecords(boolean deleted, String query, int offset, int limit, Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      recordService.getSourceRecords(query, offset, limit, deleted, tenantId)
        .map(GetSourceStorageSourceRecordsResponse::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to get source records", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void postSourceStoragePopulateTestMarcRecords(TestMarcRecordsCollection entity, Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      if (vertxContext.get(LOAD_SAMPLE_PARAMETER) != null && (Boolean) vertxContext.get(LOAD_SAMPLE_PARAMETER)) {
        List<Future> futures = new ArrayList<>();
        entity.getRawRecords().stream()
          .map(rawRecord -> {
            Record record = new Record()
              .withId(rawRecord.getId())
              .withRawRecord(rawRecord)
              .withSnapshotId(STUB_SNAPSHOT_ID)
              .withRecordType(Record.RecordType.MARC)
              .withMatchedId(rawRecord.getId());
            if (rawRecord.getContent().startsWith("{")) {
              record.setParsedRecord(new ParsedRecord().withContent(rawRecord.getContent()));
            } else {
              record = parseRecord(record);
            }
            return record;
          })
          .forEach(marcRecord -> futures.add(recordService.saveRecord(marcRecord, tenantId)));

        CompositeFuture.all(futures).setHandler(result -> {
          if (result.succeeded()) {
            asyncResultHandler.handle(Future.succeededFuture(PostSourceStoragePopulateTestMarcRecordsResponse.respond204WithTextPlain("MARC records were successfully saved")));
          } else {
            asyncResultHandler.handle(Future.succeededFuture(PostSourceStoragePopulateTestMarcRecordsResponse.respond500WithTextPlain(result.cause().getMessage())));
          }
        });
      } else {
        asyncResultHandler.handle(Future.succeededFuture(PostSourceStoragePopulateTestMarcRecordsResponse.respond400WithTextPlain("Endpoint is available only in test mode")));
      }
    } catch (Exception e) {
      LOG.error("Failed to populate test MARC records", e);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  @Override
  public void getSourceStorageFormattedRecordsById(String id, SourceStorageFormattedRecordsIdGetIdentifier identifier,
                                                   Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      recordService.getFormattedRecord(identifier, id, tenantId)
        .map(GetSourceStorageRecordsByIdResponse::respond200WithApplicationJson)
        .map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler);
    } catch (Exception e) {
      LOG.error("Failed to get record by {} id {}", e, identifier.name(), id);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  private Record parseRecord(Record record) {
    try {
      MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(record.getRawRecord().getContent().getBytes(StandardCharsets.UTF_8)));
      if (reader.hasNext()) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MarcJsonWriter writer = new MarcJsonWriter(os);
        org.marc4j.marc.Record marcRecord = reader.next();
        writer.write(marcRecord);
        record.setParsedRecord(new ParsedRecord().withContent(os.toString(StandardCharsets.UTF_8.name())));
      }
    } catch (Exception e) {
      LOG.error("Error parsing MARC record", e);
      record.setErrorRecord(new ErrorRecord().withContent(record.getRawRecord().getContent()).withDescription("Error parsing marc record"));
    }
    return record;
  }

}
