package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Data;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.handlers.actions.ModifyRecordEventHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.UPDATE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.services.handlers.actions.ModifyRecordEventHandler.MATCHED_MARC_BIB_KEY;

@RunWith(VertxUnitRunner.class)
public class ModifyRecordEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

  private static String recordId = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private RecordDao recordDao;
  private RecordService recordService;
  private ModifyRecordEventHandler modifyRecordEventHandler;
  private Record record;

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withDataType(JobProfile.DataType.MARC_BIB);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withAction(MODIFY)
    .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);

  private MarcMappingDetail marcMappingDetail = new MarcMappingDetail()
    .withOrder(0)
    .withAction(MarcMappingDetail.Action.EDIT)
    .withField(new MarcField()
      .withField("856")
      .withIndicator1(null)
      .withIndicator2(null)
      .withSubfields(Collections.singletonList(new MarcSubfield()
        .withSubfield("u")
        .withSubaction(MarcSubfield.Subaction.INSERT)
        .withPosition(MarcSubfield.Position.BEFORE_STRING)
        .withData(new Data().withText("http://libproxy.smith.edu?url=")))));

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMappingDetails(new MappingDetail()
      .withMarcMappingDetails(Collections.singletonList(marcMappingDetail)));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(jobProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(PARSED_CONTENT);
  }

  @Before
  public void setUp(TestContext context) {
    Async async = context.async();

    recordDao = new RecordDaoImpl(postgresClientFactory);
    recordService = new RecordServiceImpl(recordDao);
    modifyRecordEventHandler = new ModifyRecordEventHandler(recordService);

    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.PROCESSING_IN_PROGRESS);

    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshot)
      .compose(savedSnapshot -> recordService.saveRecord(record, TENANT_ID))
      .onSuccess(ar -> async.complete())
      .onFailure(context::fail);
  }

  @Test
  public void shouldModifyMarcRecord(TestContext context) {
    // given
    Async async = context.async();

    String expectedParsedContent = "{\"leader\":\"00107nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    HashMap<String, String> payloadContext = new HashMap<>();
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("MAPPING_PARAMS", Json.encode(new MappingParameters()));

    mappingProfile.getMappingDetails().withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));


    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), eventPayload.getEventType());

      Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateMatchedMarcRecordWithFieldFromIncomingRecord(TestContext context) {
    // given
    Async async = context.async();

    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00107nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(incomingParsedContent));
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));
    payloadContext.put("MAPPING_PARAMS", Json.encode(new MappingParameters()));

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.whenComplete((eventPayload, throwable) -> {
      context.assertNull(throwable);
      context.assertEquals(DI_SRS_MARC_BIB_RECORD_MODIFIED.value(), eventPayload.getEventType());

      Record actualRecord = Json.decodeValue(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()), Record.class);
      context.assertEquals(expectedParsedContent, actualRecord.getParsedRecord().getContent().toString());
      context.assertEquals(Record.State.ACTUAL, actualRecord.getState());
      async.complete();
    });
  }

  @Test(expected = ExecutionException.class)
  public void shouldReturnFailedFutureWhenHasNoMarcRecord() throws InterruptedException, ExecutionException, TimeoutException {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    CompletableFuture<DataImportEventPayload> future = modifyRecordEventHandler.handle(dataImportEventPayload);

    // then
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForActionProfile() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForUpdateMarcBibActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Update marc bib")
      .withAction(ActionProfile.Action.UPDATE)
      .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForActionProfile() {
    // given
    ActionProfile actionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(ActionProfile.Action.CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(actionProfile.getId())
      .withContentType(ACTION_PROFILE)
      .withContent(actionProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withContext(new HashMap<>())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper);

    // when
    boolean isEligible = modifyRecordEventHandler.isEligible(dataImportEventPayload);

    // then
    Assert.assertFalse(isEligible);
  }
}
