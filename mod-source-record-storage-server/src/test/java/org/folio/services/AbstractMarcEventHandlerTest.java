package org.folio.services;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.handlers.AbstractMarcEventHandler;
import org.folio.services.handlers.MarcAuthorityEventHandler;
import org.folio.services.handlers.MarcHoldingsEventHandler;

@RunWith(VertxUnitRunner.class)
public class AbstractMarcEventHandlerTest extends AbstractLBServiceTest {

  private static final String NOT_VALID_ENTITY_TYPE = "notValidEntityType";
  private static final String PARSED_CONTENT_WITH_ADDITIONAL_FIELDS = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"948\":{\"ind1\":\"\",\"ind2\":\"\",\"subfields\":[{\"a\":\"acf4f6e2-115c-4509-9d4c-536c758ef917\"},{\"b\":\"681394b4-10d8-4cb1-a618-0f9bd6152119\"},{\"d\":\"12345\"},{\"e\":\"lts\"},{\"x\":\"addfast\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";
  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC data";
  private static final String RECORD_ID = "acf4f6e2-115c-4509-9d4c-536c758ef917";
  private static final String SNAPSHOT_ID_1 = UUID.randomUUID().toString();
  private static final String SNAPSHOT_ID_2 = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private RecordDao recordDao;
  private Record marcAuthorityRecord;
  private Record marcHoldingsRecord;
  private MarcAuthorityEventHandler marcAuthorityEventHandler;
  private MarcHoldingsEventHandler marcHoldingsEventHandler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(RECORD_ID)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);

    recordDao = new RecordDaoImpl(postgresClientFactory);
    marcAuthorityEventHandler = new MarcAuthorityEventHandler();
    marcHoldingsEventHandler = new MarcHoldingsEventHandler();
    Async async = context.async();

    Snapshot snapshot1 = new Snapshot()
      .withJobExecutionId(SNAPSHOT_ID_1)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    Snapshot snapshot2 = new Snapshot()
      .withJobExecutionId(SNAPSHOT_ID_2)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    List<Snapshot> snapshots = new ArrayList<>();
    snapshots.add(snapshot1);
    snapshots.add(snapshot2);

    this.marcAuthorityRecord = new Record()
      .withId(RECORD_ID)
      .withMatchedId(RECORD_ID)
      .withSnapshotId(SNAPSHOT_ID_1)
      .withGeneration(1)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withId(RECORD_ID)
        .withContent(PARSED_CONTENT_WITH_ADDITIONAL_FIELDS))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId("681394b4-10d8-4cb1-a618-0f9bd6152119")
        .withInstanceHrid("12345"))
      .withState(Record.State.ACTUAL);

    this.marcHoldingsRecord = new Record()
      .withId(RECORD_ID)
      .withMatchedId(RECORD_ID)
      .withSnapshotId(SNAPSHOT_ID_1)
      .withGeneration(0)
      .withRecordType(MARC_HOLDING)
      .withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withId(RECORD_ID)
        .withContent(PARSED_CONTENT_WITH_ADDITIONAL_FIELDS))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId("681394b4-10d8-4cb1-a618-0f9bd6152119")
        .withInstanceHrid("12345"))
      .withState(Record.State.OLD);

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshots).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });

  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldHandleMarcAuthorityRecord(TestContext context) {
    shouldHandleMarcRecord(context, marcAuthorityEventHandler, EntityType.MARC_AUTHORITY, marcAuthorityRecord);
  }

  @Test
  public void shouldHandleMarcHoldingsRecord(TestContext context) {
    shouldHandleMarcRecord(context, marcHoldingsEventHandler, EntityType.MARC_HOLDINGS, marcHoldingsRecord);
  }

  @Test
  public void shouldNotHandleMarcAuthorityRecordIfContextIsNull(TestContext context) {
    shouldNotHandleMarcRecord(context, marcAuthorityEventHandler, marcAuthorityRecord, null);
  }

  @Test
  public void shouldNotHandleMarcHoldingsRecordIfContextIsNull(TestContext context) {
    shouldNotHandleMarcRecord(context, marcHoldingsEventHandler, marcHoldingsRecord, null);
  }

  @Test
  public void shouldNotHandleMarcAuthorityRecordIfContextIsEmpty(TestContext context) {
    shouldNotHandleMarcRecord(context, marcAuthorityEventHandler, marcAuthorityRecord, new HashMap<>());
  }

  @Test
  public void shouldNotHandleMarcHoldingsRecordIfContextIsEmpty(TestContext context) {
    shouldNotHandleMarcRecord(context, marcHoldingsEventHandler, marcHoldingsRecord, new HashMap<>());
  }

  @Test
  public void shouldNotHandleMarcAuthorityRecordIfContextIsNotEqualEntityType(TestContext context) {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(NOT_VALID_ENTITY_TYPE, Json.encode(marcAuthorityRecord));
    shouldNotHandleMarcRecord(context, marcAuthorityEventHandler, marcAuthorityRecord, payloadContext);
  }

  @Test
  public void shouldNotHandleMarcHoldingsRecordIfContextIsNotEqualEntityType(TestContext context) {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(NOT_VALID_ENTITY_TYPE, Json.encode(marcHoldingsRecord));
    shouldNotHandleMarcRecord(context, marcHoldingsEventHandler, marcHoldingsRecord, payloadContext);
  }

  @Test
  public void shouldReturnTrueIfMarcAuthorityPayloadIsEligible(TestContext context) {
    shouldReturnMarcPayloadIsEligible(context, EntityType.MARC_AUTHORITY.value(), marcAuthorityEventHandler, marcAuthorityRecord, true);
  }

  @Test
  public void shouldReturnTrueIfMarcHoldingsPayloadIsEligible(TestContext context) {
    shouldReturnMarcPayloadIsEligible(context, EntityType.MARC_HOLDINGS.value(), marcHoldingsEventHandler, marcHoldingsRecord, true);
  }

  @Test
  public void shouldReturnFalseIfMarcAuthorityPayloadIsEligible(TestContext context) {
    shouldReturnMarcPayloadIsEligible(context, NOT_VALID_ENTITY_TYPE, marcAuthorityEventHandler, marcAuthorityRecord, false);
  }

  @Test
  public void shouldReturnFalseIfMarcHoldingsPayloadIsEligible(TestContext context) {
    shouldReturnMarcPayloadIsEligible(context, NOT_VALID_ENTITY_TYPE, marcHoldingsEventHandler, marcHoldingsRecord, false);
  }

  @Test
  public void shouldReturnFalseIsPostProcessingNeeded(TestContext context) {
    context.assertFalse(marcAuthorityEventHandler.isPostProcessingNeeded());
    context.assertFalse(marcHoldingsEventHandler.isPostProcessingNeeded());
  }

  private void shouldReturnMarcPayloadIsEligible(TestContext context, String entityType, AbstractMarcEventHandler marcEventHandler, Record marcRecord, boolean flag) {
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(entityType, Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext);

    context.assertEquals(marcEventHandler.isEligible(dataImportEventPayload), flag);
  }

  private void shouldHandleMarcRecord(TestContext context, AbstractMarcEventHandler marcEventHandler,
                                      EntityType entityType, Record marcRecord) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(entityType.value(), Json.encode(marcRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE));

    recordDao.saveRecord(marcRecord, TENANT_ID)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          var actualRecord = new JsonObject(updatedEventPayload.getContext().get(entityType.value())).mapTo(Record.class);
          context.assertEquals(actualRecord.getId(), record.getId());
          context.assertEquals(actualRecord.getSnapshotId(), record.getSnapshotId());
          context.assertEquals(actualRecord.getRawRecord(), record.getRawRecord());
          context.assertEquals(actualRecord.getRecordType(), record.getRecordType());
          context.assertEquals(actualRecord.getErrorRecord(), record.getErrorRecord());
          async.complete();
        }));
  }

  private void shouldNotHandleMarcRecord(TestContext context, AbstractMarcEventHandler marcEventHandler,
                                         Record marcRecord, HashMap<String, String> payloadContext) {
    Async async = context.async();

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID);

    recordDao.saveRecord(marcRecord, TENANT_ID)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNotNull(throwable);
          context.assertEquals(throwable.getMessage(), PAYLOAD_HAS_NO_DATA_MSG);
          async.complete();
        }));
  }
}
