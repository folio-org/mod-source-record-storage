package org.folio.services;

import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.IdType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.handlers.actions.MarcAuthorityDeleteEventHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class MarcAuthorityDeleteEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT =
    "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordService recordService;
  private EventHandler eventHandler;
  private Record record;

  @Before
  public void before(TestContext testContext) throws IOException {
    MockitoAnnotations.openMocks(this);
    recordService = new RecordServiceImpl(new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher));
    eventHandler = new MarcAuthorityDeleteEventHandler(recordService);
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
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityId(UUID.randomUUID().toString()));
    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .onComplete(testContext.asyncAssertSuccess());
  }

  @Test
  public void shouldDeleteRecord(TestContext context) {
    Async async = context.async();
    // given
    record.setParsedRecord(new ParsedRecord().withId(record.getId()).withContent(PARSED_CONTENT));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("MATCHED_MARC_AUTHORITY", Json.encode(record));
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
    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordService.saveRecord(record, okapiHeaders)
      // when
      .onSuccess(ar -> eventHandler.handle(dataImportEventPayload)
        // then
        .whenComplete((eventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value(), eventPayload.getEventType());
          context.assertNull(eventPayload.getContext().get("MATCHED_MARC_AUTHORITY"));
          context.assertEquals(record.getExternalIdsHolder().getAuthorityId(), eventPayload.getContext().get("AUTHORITY_RECORD_ID"));
          recordService.getRecordById(record.getId(), TENANT_ID)
            .onSuccess(optionalDeletedRecord -> {
              context.assertTrue(optionalDeletedRecord.isPresent());
              Record deletedRecord = optionalDeletedRecord.get();
              context.assertTrue(deletedRecord.getDeleted());
              context.assertEquals(deletedRecord.getLeaderRecordStatus(), "d");
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
  public void shouldHandleErrorDuringRecordDeletion(TestContext context) {
    Async async = context.async();
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("MATCHED_MARC_AUTHORITY", Json.encode(record));
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

    RecordService spyRecordService = spy(recordService);
    doReturn(Future.failedFuture(new RuntimeException("Deletion error")))
      .when(spyRecordService).deleteRecordById(anyString(), any(IdType.class), anyMap());

    EventHandler spyEventHandler = new MarcAuthorityDeleteEventHandler(spyRecordService);

    // when
    CompletableFuture<DataImportEventPayload> future = spyEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNotNull(throwable);
      context.assertEquals("Deletion error", throwable.getMessage());
      async.complete();
    });
  }

  @Test
  public void shouldCompleteIfNoRecordStored(TestContext context) {
    Async async = context.async();
    // given
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put("MATCHED_MARC_AUTHORITY", Json.encode(record));
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
      context.assertNull(throwable);
      async.complete();
    });
  }

  @Test
  public void actionProfileIsEligible() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Delete marc authority")
      .withAction(DELETE)
      .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = eventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void actionProfileIsNotEligible() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Delete marc authority")
      .withAction(UPDATE)
      .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = eventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertFalse(isEligible);
  }
}
