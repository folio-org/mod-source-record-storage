package org.folio.services;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.handlers.actions.MarcAuthorityDeleteEventHandler;
import org.folio.services.util.TypeConnection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;

@RunWith(VertxUnitRunner.class)
public class MarcAuthorityDeleteEventHandlerTest extends AbstractLBServiceTest {
  private final RecordService recordService = new RecordServiceImpl(new RecordDaoImpl(postgresClientFactory));
  private final EventHandler eventHandler = new MarcAuthorityDeleteEventHandler(recordService);
  private Record record;

  @Before
  public void before(TestContext testContext) throws IOException {
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    String recordId = UUID.randomUUID().toString();
    RawRecord rawRecord = new RawRecord().withId(recordId).withContent("");
    ParsedRecord parsedRecord = new ParsedRecord().withId(recordId).withContent(new JsonObject().encodePrettily());
    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .onComplete(testContext.asyncAssertSuccess());
  }

  @Test
  public void shouldDeleteRecord(TestContext context) {
    Async async = context.async();
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("MATCHED_" + TypeConnection.MARC_AUTHORITY.getMarcType(), Json.encode(record));
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(new ActionProfile()
          .withId(UUID.randomUUID().toString())
          .withName("Delete Marc Authorities")
          .withAction(DELETE)
          .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY)
        )
      );
    recordService.saveRecord(record, TENANT_ID)
      // when
      .onSuccess(ar -> eventHandler.handle(dataImportEventPayload)
        // then
        .whenComplete((eventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value(), eventPayload.getEventType());
          recordService.getRecordById(record.getId(), TENANT_ID)
            .onSuccess(optionalRecord -> {
              context.assertTrue(optionalRecord.isEmpty());
              async.complete();
            });
        })
      );
  }

  @Test
  public void shouldCompleteExceptionallyIfNoRecordInPayload(TestContext context) {
    Async async = context.async();
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(new ActionProfile()
          .withId(UUID.randomUUID().toString())
          .withName("Delete Marc Authorities")
          .withAction(DELETE)
          .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY)
        )
      );
    // when
    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals("Failed to handle event payload, cause event payload context does not contain required data to modify MARC record", throwable.getMessage());
      async.complete();
    });
  }

  @Test
  public void shouldCompleteExceptionallyIfNoRecordStored(TestContext context) {
    Async async = context.async();
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("MATCHED_" + TypeConnection.MARC_AUTHORITY.getMarcType(), Json.encode(record));
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(new ActionProfile()
          .withId(UUID.randomUUID().toString())
          .withName("Delete Marc Authorities")
          .withAction(DELETE)
          .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY)
        )
      );
    // when
    CompletableFuture<DataImportEventPayload> future = eventHandler.handle(dataImportEventPayload);
    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals("Error while deleting MARC record, record is not found", throwable.getMessage());
      async.complete();
    });
  }
}
