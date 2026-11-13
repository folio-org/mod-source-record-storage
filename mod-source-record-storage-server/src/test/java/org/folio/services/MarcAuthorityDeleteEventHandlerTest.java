package org.folio.services;

import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxTestContext;
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
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.caches.ConsortiumConfigurationCache;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.handlers.actions.MarcAuthorityDeleteEventHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class MarcAuthorityDeleteEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT =
    "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  @Mock
  private ConsortiumConfigurationCache consortiumConfigurationCache;
  private RecordService recordService;
  private EventHandler eventHandler;
  private Record record;

  @BeforeEach
  void before(VertxTestContext testContext) {
    recordService = new RecordServiceImpl(new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher),
      consortiumConfigurationCache);
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
      .onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldDeleteRecord(VertxTestContext testContext) {
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
    var okapiHeaders = Map.of(XOkapiHeaders.TENANT, TENANT_ID);
    recordService.saveRecord(record, okapiHeaders)
      // when
      .onFailure(testContext::failNow)
      .onSuccess(ar -> eventHandler.handle(dataImportEventPayload)
        // then
        .whenComplete((eventPayload, throwable) -> {
          assertNull(throwable);
          assertEquals(DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value(), eventPayload.getEventType());
          assertNull(eventPayload.getContext().get("MATCHED_MARC_AUTHORITY"));
          var deletedRecordJson = eventPayload.getContext().get("DELETED_MARC_AUTHORITY");
          assertNotNull(deletedRecordJson);
          assertEquals(record.getExternalIdsHolder().getAuthorityId(), eventPayload.getContext().get("AUTHORITY_RECORD_ID"));
          recordService.getRecordById(record.getId(), TENANT_ID)
            .onComplete(optionalDeletedRecordAr -> {
              assertTrue(optionalDeletedRecordAr.succeeded());
              assertTrue(optionalDeletedRecordAr.result().isPresent());
              Record deletedRecord = optionalDeletedRecordAr.result().get();
//              assertTrue(deletedRecord.getDeleted());
//              assertEquals(deletedRecord.getLeaderRecordStatus(), "d");
              testContext.completeNow();
            });
        })
      );
  }

  @Test
  void shouldCompleteExceptionallyIfNoRecordInPayload(VertxTestContext testContext) {
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
      assertNotNull(throwable);
      assertEquals("Failed to handle event payload, cause event payload context does not contain required data to modify MARC record", throwable.getMessage());
      testContext.completeNow();
    });
  }

  @Test
  void shouldHandleErrorDuringRecordDeletion(VertxTestContext testContext) {
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
      assertNotNull(throwable);
      assertEquals("Deletion error", throwable.getMessage());
      testContext.completeNow();
    });
  }

  @Test
  void shouldCompleteIfNoRecordStored(VertxTestContext testContext) {
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
      assertNull(throwable);
      testContext.completeNow();
    });
  }

  @Test
  void actionProfileIsEligible() {
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
    assertTrue(isEligible);
  }

  @Test
  void actionProfileIsNotEligible() {
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
    assertFalse(isEligible);
  }
}
