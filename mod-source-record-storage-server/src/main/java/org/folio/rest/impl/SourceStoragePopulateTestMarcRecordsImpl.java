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
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.TestMarcRecordsCollection;
import org.folio.rest.jaxrs.resource.SourceStoragePopulateTestMarcRecords;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.RecordService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SourceStoragePopulateTestMarcRecordsImpl implements SourceStoragePopulateTestMarcRecords {

  private static final Logger LOG = LogManager.getLogger();

  @Autowired
  private RecordService recordService;

  private final String tenantId;

  public SourceStoragePopulateTestMarcRecordsImpl(Vertx vertx, String tenantId) { // NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
  }

  @Override
  public void postSourceStoragePopulateTestMarcRecords(TestMarcRecordsCollection entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      if (vertxContext.get(LOAD_SAMPLE_PARAMETER) != null && (Boolean) vertxContext.get(LOAD_SAMPLE_PARAMETER)) {
        List<Future> futures = new ArrayList<>();
        entity.getRawRecords().stream()
          .map(rawRecord -> {
            Record record = new Record()
              .withId(rawRecord.getId())
              .withMatchedId(rawRecord.getId())
              .withSnapshotId(STUB_SNAPSHOT.getJobExecutionId())
              .withRecordType(RecordType.MARC_BIB)
              .withState(State.ACTUAL)
              .withRawRecord(rawRecord);
            if (rawRecord.getContent().startsWith("{")) {
              record.setParsedRecord(new ParsedRecord().withContent(rawRecord.getContent()));
            } else {
              parseRecord(record);
            }
            return record;
          })
          .forEach(marcRecord -> futures.add(recordService.saveRecord(marcRecord, tenantId)));

        GenericCompositeFuture.all(futures).onComplete(result -> {
          if (result.succeeded()) {
            asyncResultHandler.handle(Future.succeededFuture(PostSourceStoragePopulateTestMarcRecordsResponse.respond204()));
          } else {
            asyncResultHandler.handle(Future.succeededFuture(PostSourceStoragePopulateTestMarcRecordsResponse
              .respond500WithTextPlain(result.cause().getMessage())));
          }
        });
      } else {
        asyncResultHandler.handle(Future.succeededFuture(PostSourceStoragePopulateTestMarcRecordsResponse
          .respond400WithTextPlain("Endpoint is available only in test mode")));
      }
    });
  }

  private void parseRecord(Record record) {
    try {
      record.setParsedRecord(new ParsedRecord().withContent(MarcUtil.rawMarcToMarcJson(record.getRawRecord().getContent())));
    } catch (Exception e) {
      LOG.error("Error parsing MARC record", e);
      record.setErrorRecord(new ErrorRecord().withContent(record.getRawRecord().getContent()).withDescription("Error parsing marc record"));
    }
  }

}
