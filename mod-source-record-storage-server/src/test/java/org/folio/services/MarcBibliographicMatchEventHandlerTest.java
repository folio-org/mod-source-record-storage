package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.handlers.MarcBibliographicMatchEventHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;

@RunWith(VertxUnitRunner.class)
public class MarcBibliographicMatchEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT_WITH_ADDITIONAL_FIELDS = "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"948\":{\"ind1\":\"\",\"ind2\":\"\",\"subfields\":[{\"a\":\"acf4f6e2-115c-4509-9d4c-536c758ef917\"},{\"b\":\"681394b4-10d8-4cb1-a618-0f9bd6152119\"},{\"d\":\"12345\"},{\"e\":\"lts\"},{\"x\":\"addfast\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";

  public static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";

  private RecordDao recordDao;

  private MarcBibliographicMatchEventHandler marcBibliographicMatchEventHandler;

  private static RawRecord rawRecord;

  private static String recordId = "acf4f6e2-115c-4509-9d4c-536c758ef917";

  private String snapshotId1 = UUID.randomUUID().toString();
  private String snapshotId2 = UUID.randomUUID().toString();

  private Record record;

  private Record secondRecord;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);

    recordDao = new RecordDaoImpl(postgresClientFactory);
    marcBibliographicMatchEventHandler = new MarcBibliographicMatchEventHandler(recordDao, vertx);
    Async async = context.async();

    Snapshot snapshot1 = new Snapshot()
      .withJobExecutionId(snapshotId1)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    Snapshot snapshot2 = new Snapshot()
      .withJobExecutionId(snapshotId2)
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    List<Snapshot> snapshots = new ArrayList<>();
    snapshots.add(snapshot1);
    snapshots.add(snapshot2);

    this.record = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(1)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withId(recordId)
        .withContent(PARSED_CONTENT_WITH_ADDITIONAL_FIELDS))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId("681394b4-10d8-4cb1-a618-0f9bd6152119")
        .withInstanceHrid("12345"))
    .withState(Record.State.ACTUAL);

    this.secondRecord = new Record()
      .withId(String.valueOf(UUID.randomUUID()))
      .withMatchedId(recordId)
      .withSnapshotId(snapshotId1)
      .withGeneration(0)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(new ParsedRecord()
        .withId(recordId)
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
  public void shouldMatchByMatchedIdField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("s"))))
            .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("948"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a"))))))));

    RecordDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), secondRecord).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });

    recordDao.saveRecord(record, TENANT_ID)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcBibliographicMatchEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(),
            DI_SRS_MARC_BIB_RECORD_MATCHED.value());
          context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_BIB_KEY)).mapTo(Record.class), record);
          async.complete();
        }));
  }

  @Test
  public void shouldMatchByInstanceIdField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("i"))))
            .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("948"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("b"))))))));

    RecordDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), secondRecord)
      .compose(v -> recordDao.saveRecord(record, TENANT_ID))
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcBibliographicMatchEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(),
            DI_SRS_MARC_BIB_RECORD_MATCHED.value());
          context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_BIB_KEY)).mapTo(Record.class), record);
          async.complete();
        }));
  }

  @Test
  public void shouldMatchByInstanceHridField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("001"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue(""))))
            .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("948"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("d"))))))));

    RecordDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), secondRecord)
      .compose(v -> recordDao.saveRecord(record, TENANT_ID))
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcBibliographicMatchEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(),
            DI_SRS_MARC_BIB_RECORD_MATCHED.value());
          context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_BIB_KEY)).mapTo(Record.class), record);
          async.complete();
        }));
  }

  @Test
  public void shouldNotMatchByMatchedIdField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("s"))))
            .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("948"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("b"))))))));

    recordDao.saveRecord(record, TENANT_ID)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcBibliographicMatchEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(),
            DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value());
          async.complete();
        }));
  }

  @Test
  public void shouldNotMatchByMatchedIdFieldIfNotMatch(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("r"))))
            .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("948"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("d"))))))));

    recordDao.saveRecord(record, TENANT_ID)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> marcBibliographicMatchEventHandler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(),
            DI_SRS_MARC_BIB_RECORD_NOT_MATCHED.value());
          async.complete();
        }));
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForProfile() {
    MatchProfile matchProfile = new MatchProfile()
      .withId(UUID.randomUUID().toString())
      .withName("MARC-MARC matching")
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withContent(matchProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = marcBibliographicMatchEventHandler.isEligible(dataImportEventPayload);

    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForProfile() {
    MatchProfile matchProfile = new MatchProfile()
      .withId(UUID.randomUUID().toString())
      .withName("MARC-MARC matching")
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.HOLDINGS);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withContent(matchProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = marcBibliographicMatchEventHandler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenNotMatchProfileForProfile() {
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(mappingProfile.getId())
      .withContentType(MAPPING_PROFILE)
      .withContent(mappingProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType("DI_SRS_MARC_BIB_RECORD_CREATED")
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = marcBibliographicMatchEventHandler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }
}
