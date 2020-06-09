package org.folio.rest.impl;

import static org.folio.rest.impl.ModTenantAPI.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.TestMarcRecordsCollection;
import org.folio.rest.jaxrs.resource.LbSourceStoragePopulateTestMarcRecords;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.LbRecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class LbSourceStoragePopulateTestMarcRecordsImpl implements LbSourceStoragePopulateTestMarcRecords {

  private static final Logger LOG = LoggerFactory.getLogger(LbSourceStoragePopulateTestMarcRecordsImpl.class);

  @Autowired
  private LbRecordService recordService;

  private final String tenantId;

  public LbSourceStoragePopulateTestMarcRecordsImpl(Vertx vertx, String tenantId) { // NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postLbSourceStoragePopulateTestMarcRecords(TestMarcRecordsCollection entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      if (vertxContext.get(LOAD_SAMPLE_PARAMETER) != null && (Boolean) vertxContext.get(LOAD_SAMPLE_PARAMETER)) {
        @SuppressWarnings("squid:S3740")
        List<Future> futures = new ArrayList<>();
        entity.getRawRecords().stream()
          .map(rawRecord -> {
            Record record = new Record()
              .withId(rawRecord.getId())
              .withMatchedId(rawRecord.getId())
              .withSnapshotId(STUB_SNAPSHOT.getJobExecutionId())
              .withRecordType(Record.RecordType.MARC)
              .withState(State.ACTUAL)
              .withRawRecord(rawRecord);
            if (rawRecord.getContent().startsWith("{")) {
              record.setParsedRecord(new ParsedRecord().withContent(rawRecord.getContent()));
            } else {
              record = parseRecord(record);
            }
            return record;
          })
          .forEach(marcRecord -> futures.add(recordService.saveRecord(marcRecord, tenantId)));

        CompositeFuture.all(futures).onComplete(result -> {
          if (result.succeeded()) {
            asyncResultHandler.handle(Future.succeededFuture(PostLbSourceStoragePopulateTestMarcRecordsResponse.respond204()));
          } else {
            result.cause().printStackTrace();
            asyncResultHandler.handle(Future.succeededFuture(PostLbSourceStoragePopulateTestMarcRecordsResponse.respond500WithTextPlain(result.cause().getMessage())));
          }
        });
      } else {
        asyncResultHandler.handle(Future.succeededFuture(PostLbSourceStoragePopulateTestMarcRecordsResponse
          .respond400WithTextPlain("Endpoint is available only in test mode")));
      }
    });
  }

  private Record parseRecord(Record record) {
    try {
      record.setParsedRecord(new ParsedRecord().withContent(MarcUtil.rawMarcToMarcJson(record.getRawRecord().getContent())));
    } catch (Exception e) {
      LOG.error("Error parsing MARC record", e);
      record.setErrorRecord(new ErrorRecord().withContent(record.getRawRecord().getContent()).withDescription("Error parsing marc record"));
    }
    return record;
  }

}