package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
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
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.caches.MappingParametersSnapshotCache;
import org.folio.services.handlers.actions.MarcBibUpdateModifyEventHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static org.folio.ActionProfile.Action.MODIFY;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.MappingDetail.MarcMappingOption.UPDATE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;

@RunWith(VertxUnitRunner.class)
public class MarcBibUpdateModifyEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
  private static final String MAPPING_METADATA__URL = "/mapping-metadata";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";

  private static String recordId = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;

  private RecordDao recordDao;
  private RecordService recordService;
  private MarcBibUpdateModifyEventHandler modifyRecordEventHandler;
  private Snapshot snapshotForRecordUpdate;
  private Record record;

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

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

  private List<MarcMappingDetail> marcBibMappingDetail = List.of(
    new MarcMappingDetail().withOrder(0).withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("015").withIndicator1("*").withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*").withData(null).withSubaction(null).withPosition(null)))),
    new MarcMappingDetail().withOrder(1).withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("016").withIndicator1("*").withIndicator2("*")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*").withData(null).withSubaction(null).withPosition(null)))),
    new MarcMappingDetail().withOrder(2).withAction(MarcMappingDetail.Action.EDIT)
      .withField(new MarcField().withField("035").withIndicator1(null).withIndicator2(null)
        .withSubfields(Collections.singletonList(new MarcSubfield()
          .withSubfield("a").withSubaction(MarcSubfield.Subaction.REPLACE).withPosition(null)
          .withData(new Data().withText(null).withFind("(OCoLC)on").withReplaceWith("(OCoLC)")))))
  );

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMappingDetails(new MappingDetail()
      .withMarcMappingDetails(Collections.singletonList(marcMappingDetail)));

  private MappingProfile marcBibModifyMappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Modify MARC Bibs")
    .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(MARC_BIBLIOGRAPHIC)
    .withMappingDetails(new MappingDetail()
      .withMarcMappingDetails(marcBibMappingDetail)
      .withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY));

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

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(PARSED_CONTENT);
  }

  @Before
  public void setUp(TestContext context) {
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA__URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))))));

    recordDao = new RecordDaoImpl(postgresClientFactory);
    recordService = new RecordServiceImpl(recordDao);
    modifyRecordEventHandler = new MarcBibUpdateModifyEventHandler(recordService, new MappingParametersSnapshotCache(vertx), vertx);

    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    snapshotForRecordUpdate = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS);

    record = new Record()
      .withId(recordId)
      .withSnapshotId(snapshot.getJobExecutionId())
      .withGeneration(0)
      .withMatchedId(recordId)
      .withRecordType(MARC_BIB)
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord);

    ReactiveClassicGenericQueryExecutor queryExecutor = postgresClientFactory.getQueryExecutor(TENANT_ID);
    SnapshotDaoUtil.save(queryExecutor, snapshot)
      .compose(v -> recordService.saveRecord(record, TENANT_ID))
      .compose(v -> SnapshotDaoUtil.save(queryExecutor, snapshotForRecordUpdate))
      .onComplete(context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    SnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID))
      .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void shouldModifyMarcRecord(TestContext context) {
    // given
    Async async = context.async();

    String expectedParsedContent = "{\"leader\":\"00107nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    HashMap<String, String> payloadContext = new HashMap<>();
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    mappingProfile.getMappingDetails().withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(record.getSnapshotId())
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

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
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
      context.assertEquals(dataImportEventPayload.getJobExecutionId(), actualRecord.getSnapshotId());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateMarcRecordAndRemove003Field(TestContext context) {
    // given
    Async async = context.async();

    String incomingParsedContent = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406512\"},{\"003\":\"OCLC\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00107nam  22000491a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"856\":{\"subfields\":[{\"u\":\"http://libproxy.smith.edu?url=example.com\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(incomingParsedContent));
    record.getParsedRecord().setContent(Json.encode(record.getParsedRecord().getContent()));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    payloadContext.put(MATCHED_MARC_BIB_KEY, Json.encode(record));

    mappingProfile.getMappingDetails().withMarcMappingOption(UPDATE);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(mappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(mappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
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
  public void shouldModifyMarcBibRecordAndNotRemove003Field(TestContext context) {
    // given
    Async async = context.async();

    String incomingParsedContent = "{\"leader\":\"05490cam a2200877Ia 4500\",\"fields\":[{\"001\":\"ocn297303223\"},{\"003\":\"OCoLC\"},{\"005\":\"20210226180151.2\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr unu---uuaua\"},{\"008\":\"090107s1954    dcua    ob    100 0 eng d\"},{\"010\":{\"subfields\":[{\"a\":\"   55000367 \"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"015\":{\"subfields\":[{\"a\":\"B67-25185 \"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"016\":{\"subfields\":[{\"a\":\"000002550474\"},{\"2\":\"AU\"}],\"ind1\":\"7\",\"ind2\":\" \"}},{\"019\":{\"subfields\":[{\"a\":\"780330352\"},{\"a\":\"1057910652\"},{\"a\":\"1078369885\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"9780841221574\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"084122157X\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"0841200122\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"9780841200128\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"z\":\"9780841200128\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"z\":\"0841200122\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"024\":{\"subfields\":[{\"a\":\"9780841200128\"}],\"ind1\":\"3\",\"ind2\":\" \"}},{\"029\":{\"subfields\":[{\"a\":\"AU@\"},{\"b\":\"000048638076\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)on297303223\"},{\"z\":\"(OCoLC)780330352\"},{\"z\":\"(OCoLC)1057910652\"},{\"z\":\"(OCoLC)1078369885\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"10.1021/ba-1954-0011\"}],\"ind1\":\"9\",\"ind2\":\" \"}},{\"037\":{\"subfields\":[{\"b\":\"00001081\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"040\":{\"subfields\":[{\"a\":\"OCLCE\"},{\"b\":\"eng\"},{\"e\":\"pn\"},{\"c\":\"OCLCE\"},{\"d\":\"MUU\"},{\"d\":\"OCLCQ\"},{\"d\":\"COO\"},{\"d\":\"OCLCF\"},{\"d\":\"OCLCA\"},{\"d\":\"AU@\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCL\"},{\"d\":\"ACY\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCA\"},{\"d\":\"YOU\"},{\"d\":\"CASSC\"},{\"d\":\"OCLCA\"},{\"d\":\"MERER\"},{\"d\":\"OCLCO\"},{\"d\":\"CUY\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"049\":{\"subfields\":[{\"a\":\"AUMM\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"050\":{\"subfields\":[{\"a\":\"QD1\"},{\"b\":\".A355 no. 11\"}],\"ind1\":\" \",\"ind2\":\"4\"}},{\"060\":{\"subfields\":[{\"a\":\"QU 188\"},{\"b\":\"N285 1954\"}],\"ind1\":\" \",\"ind2\":\"4\"}},{\"072\":{\"subfields\":[{\"a\":\"SCI\"},{\"x\":\"013050\"},{\"2\":\"bisacsh\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"082\":{\"subfields\":[{\"a\":\"541.3452\"},{\"a\":\"541.375*\"}],\"ind1\":\"0\",\"ind2\":\"4\"}},{\"083\":{\"subfields\":[{\"z\":\"2\"},{\"a\":\"4947\"},{\"2\":\"22\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"084\":{\"subfields\":[{\"a\":\"SCI007000\"},{\"2\":\"bisacsh\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"092\":{\"subfields\":[{\"a\":\"551.46 ǂ2 23/eng/2012\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"111\":{\"subfields\":[{\"a\":\"Symposium on Natural Plant Hydrocolloids\"},{\"d\":\"(1952 :\"},{\"c\":\"Atlantic City, N.J.)\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"245\":{\"subfields\":[{\"a\":\"Natural plant hydrocolloids :\"},{\"b\":\"a collection of papers comprising the Symposium on Natural Plant Hydrocolloids, presented before the Divisions of Colloid Chemistry and Agricultural and Food Chemistry at the 122nd meeting of the American Chemical Society, Atlantic City, N.J., September 1952.\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"260\":{\"subfields\":[{\"a\":\"Washington, D.C. :\"},{\"b\":\"American Chemical Society,\"},{\"c\":\"1954.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"300\":{\"subfields\":[{\"a\":\"1 online resource (iii, 103 pages) :\"},{\"b\":\"illustrations.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"336\":{\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"337\":{\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"338\":{\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"490\":{\"subfields\":[{\"a\":\"Advances in chemistry series,\"},{\"x\":\"0065-2393 ;\"},{\"v\":\"no. 11\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"504\":{\"subfields\":[{\"a\":\"Includes bibliographical references.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"505\":{\"subfields\":[{\"t\":\"Introductory Remarks /\"},{\"r\":\"STOLOFF, LEONARD /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch001 --\"},{\"t\":\"Calcium Pectinates, Their Preparation and Uses /\"},{\"r\":\"WOODMANSEE, CLINTON W.; BAKER, GEORCE L. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch002 --\"},{\"t\":\"Factors Influencing Gelation with Pectin /\"},{\"r\":\"OWENS, HARRY S.; SWENSON, HAROLD A.; SCHULTZ, THOMAS H. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch003 --\"},{\"t\":\"Agar Since 1943 /\"},{\"r\":\"SELBY, HORACE H. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch004 --\"},{\"t\":\"Technology of Gum Arabic /\"},{\"r\":\"MANTELL, CHARLES L. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch005 --\"},{\"t\":\"Chemistry, Properties, and Application Of Gum Karaya /\"},{\"r\":\"GOLDSTEIN, ARTHUR M. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch006 --\"},{\"t\":\"History, Production, and Uses of Tragacanth /\"},{\"r\":\"BEACH, D. C. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch007 --\"},{\"t\":\"Guar Gum, Locust Bean Gum, and Others /\"},{\"r\":\"WHISTLER, ROY L. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch008 --\"},{\"t\":\"Some Properties of Locust Bean Gum /\"},{\"r\":\"DEUEL, HANS; NEUKOM, HANS /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch009 --\"},{\"t\":\"Observations on Pectic Substances /\"},{\"r\":\"DEUEL, HANS; SOLMS, JÜRG /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch010 --\"},{\"t\":\"Algin in Review /\"},{\"r\":\"STEINER, ARNOLD B.; McNEELY, WILLIAM H. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch011 --\"},{\"t\":\"Alginates from Common British Brown Marine Algae /\"},{\"r\":\"BLACK, W. A . P.; WOODWARD, F. N. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch012 --\"},{\"t\":\"Irish Moss Extractives /\"},{\"r\":\"STOLOFF, LEONARD /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch013 --\"},{\"t\":\"Effect of Different Ions on Gel Strength Of Red Seaweed Extracts /\"},{\"r\":\"MARSHALL, S. M.; ORR, A. P. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch014\"}],\"ind1\":\"0\",\"ind2\":\"0\"}},{\"506\":{\"subfields\":[{\"a\":\"Online full text is restricted to subscribers.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"583\":{\"subfields\":[{\"a\":\"committed to retain\"},{\"c\":\"20160630\"},{\"d\":\"20310630\"},{\"f\":\"EAST\"},{\"u\":\"https://eastlibraries.org/retained-materials\"},{\"5\":\"DCHS\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"588\":{\"subfields\":[{\"a\":\"Print version record.\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Biocolloids\"},{\"v\":\"Congresses.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"SCIENCE\"},{\"x\":\"Chemistry\"},{\"x\":\"Physical & Theoretical.\"},{\"2\":\"bisacsh\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"650\":{\"subfields\":[{\"a\":\"Biocolloids.\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst00831997\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"650\":{\"subfields\":[{\"a\":\"Colloids\"},{\"x\":\"chemistry.\"}],\"ind1\":\"1\",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Colloids\"},{\"x\":\"economics.\"}],\"ind1\":\"1\",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Pectins\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Agar\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Gum Arabic\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Karaya Gum\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Tragacanth\"},{\"x\":\"history.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Tragacanth\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Plant Gums\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Alginates\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Phaeophyta\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Chondrus\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Rhodophyta\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"655\":{\"subfields\":[{\"a\":\"Electronic books.\"}],\"ind1\":\" \",\"ind2\":\"4\"}},{\"655\":{\"subfields\":[{\"a\":\"Conference papers and proceedings.\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst01423772\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"710\":{\"subfields\":[{\"a\":\"American Chemical Society.\"},{\"b\":\"Division of Agricultural and Food Chemistry.\"},{\"0\":\"http://id.loc.gov/authorities/names/n79007703\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"American Chemical Society.\"},{\"b\":\"Division of Colloid and Surface Chemistry.\"},{\"0\":\"http://id.loc.gov/authorities/names/n80109319\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"American Chemical Society.\"},{\"b\":\"Meeting\"},{\"n\":\"(122nd :\"},{\"d\":\"1952 :\"},{\"c\":\"Atlantic City, N.J.)\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"776\":{\"subfields\":[{\"i\":\"Print version:\"},{\"a\":\"American Chemical Society. Division of Colloid and Surface Chemistry.\"},{\"t\":\"Natural plant hydrocolloids.\"},{\"d\":\"Washington, D.C. : American Chemical Society, 1954\"},{\"w\":\"(DLC)   55000367\"},{\"w\":\"(OCoLC)280432\"}],\"ind1\":\"0\",\"ind2\":\"8\"}},{\"830\":{\"subfields\":[{\"a\":\"Advances in chemistry series ;\"},{\"v\":\"11.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"850\":{\"subfields\":[{\"a\":\"AAP\"},{\"a\":\"CU\"},{\"a\":\"DLC\"},{\"a\":\"MiU \"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://silk.library.umass.edu/login?url=https://pubs.acs.org/doi/book/10.1021/ba-1954-0011\"},{\"z\":\"UMass: Link to resource\"}],\"ind1\":\"4\",\"ind2\":\"0\"}},{\"891\":{\"subfields\":[{\"9\":\"853\"},{\"8\":\"1\"},{\"a\":\"(year/year)\"}],\"ind1\":\"3\",\"ind2\":\"3\"}},{\"938\":{\"subfields\":[{\"a\":\"ebrary\"},{\"b\":\"EBRY\"},{\"n\":\"ebr10728707\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"05375cam a2200841Ia 4500\",\"fields\":[{\"001\":\"ocn297303223\"},{\"003\":\"OCoLC\"},{\"005\":\"20210226180151.2\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr unu---uuaua\"},{\"008\":\"090107s1954    dcua    ob    100 0 eng d\"},{\"010\":{\"subfields\":[{\"a\":\"   55000367 \"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"019\":{\"subfields\":[{\"a\":\"780330352\"},{\"a\":\"1057910652\"},{\"a\":\"1078369885\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"9780841221574\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"084122157X\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"0841200122\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"a\":\"9780841200128\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"z\":\"9780841200128\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"020\":{\"subfields\":[{\"z\":\"0841200122\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"024\":{\"subfields\":[{\"a\":\"9780841200128\"}],\"ind1\":\"3\",\"ind2\":\" \"}},{\"029\":{\"subfields\":[{\"a\":\"AU@\"},{\"b\":\"000048638076\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)297303223\"},{\"z\":\"(OCoLC)780330352\"},{\"z\":\"(OCoLC)1057910652\"},{\"z\":\"(OCoLC)1078369885\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"10.1021/ba-1954-0011\"}],\"ind1\":\"9\",\"ind2\":\" \"}},{\"037\":{\"subfields\":[{\"b\":\"00001081\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"040\":{\"subfields\":[{\"a\":\"OCLCE\"},{\"b\":\"eng\"},{\"e\":\"pn\"},{\"c\":\"OCLCE\"},{\"d\":\"MUU\"},{\"d\":\"OCLCQ\"},{\"d\":\"COO\"},{\"d\":\"OCLCF\"},{\"d\":\"OCLCA\"},{\"d\":\"AU@\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCL\"},{\"d\":\"ACY\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCA\"},{\"d\":\"YOU\"},{\"d\":\"CASSC\"},{\"d\":\"OCLCA\"},{\"d\":\"MERER\"},{\"d\":\"OCLCO\"},{\"d\":\"CUY\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"049\":{\"subfields\":[{\"a\":\"AUMM\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"050\":{\"subfields\":[{\"a\":\"QD1\"},{\"b\":\".A355 no. 11\"}],\"ind1\":\" \",\"ind2\":\"4\"}},{\"060\":{\"subfields\":[{\"a\":\"QU 188\"},{\"b\":\"N285 1954\"}],\"ind1\":\" \",\"ind2\":\"4\"}},{\"072\":{\"subfields\":[{\"a\":\"SCI\"},{\"x\":\"013050\"},{\"2\":\"bisacsh\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"082\":{\"subfields\":[{\"a\":\"541.3452\"},{\"a\":\"541.375*\"}],\"ind1\":\"0\",\"ind2\":\"4\"}},{\"083\":{\"subfields\":[{\"z\":\"2\"},{\"a\":\"4947\"},{\"2\":\"22\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"084\":{\"subfields\":[{\"a\":\"SCI007000\"},{\"2\":\"bisacsh\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"092\":{\"subfields\":[{\"a\":\"551.46 ǂ2 23/eng/2012\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"111\":{\"subfields\":[{\"a\":\"Symposium on Natural Plant Hydrocolloids\"},{\"d\":\"(1952 :\"},{\"c\":\"Atlantic City, N.J.)\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"245\":{\"subfields\":[{\"a\":\"Natural plant hydrocolloids :\"},{\"b\":\"a collection of papers comprising the Symposium on Natural Plant Hydrocolloids, presented before the Divisions of Colloid Chemistry and Agricultural and Food Chemistry at the 122nd meeting of the American Chemical Society, Atlantic City, N.J., September 1952.\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"260\":{\"subfields\":[{\"a\":\"Washington, D.C. :\"},{\"b\":\"American Chemical Society,\"},{\"c\":\"1954.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"300\":{\"subfields\":[{\"a\":\"1 online resource (iii, 103 pages) :\"},{\"b\":\"illustrations.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"336\":{\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"337\":{\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"338\":{\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"490\":{\"subfields\":[{\"a\":\"Advances in chemistry series,\"},{\"x\":\"0065-2393 ;\"},{\"v\":\"no. 11\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"504\":{\"subfields\":[{\"a\":\"Includes bibliographical references.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"505\":{\"subfields\":[{\"t\":\"Introductory Remarks /\"},{\"r\":\"STOLOFF, LEONARD /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch001 --\"},{\"t\":\"Calcium Pectinates, Their Preparation and Uses /\"},{\"r\":\"WOODMANSEE, CLINTON W.; BAKER, GEORCE L. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch002 --\"},{\"t\":\"Factors Influencing Gelation with Pectin /\"},{\"r\":\"OWENS, HARRY S.; SWENSON, HAROLD A.; SCHULTZ, THOMAS H. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch003 --\"},{\"t\":\"Agar Since 1943 /\"},{\"r\":\"SELBY, HORACE H. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch004 --\"},{\"t\":\"Technology of Gum Arabic /\"},{\"r\":\"MANTELL, CHARLES L. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch005 --\"},{\"t\":\"Chemistry, Properties, and Application Of Gum Karaya /\"},{\"r\":\"GOLDSTEIN, ARTHUR M. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch006 --\"},{\"t\":\"History, Production, and Uses of Tragacanth /\"},{\"r\":\"BEACH, D. C. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch007 --\"},{\"t\":\"Guar Gum, Locust Bean Gum, and Others /\"},{\"r\":\"WHISTLER, ROY L. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch008 --\"},{\"t\":\"Some Properties of Locust Bean Gum /\"},{\"r\":\"DEUEL, HANS; NEUKOM, HANS /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch009 --\"},{\"t\":\"Observations on Pectic Substances /\"},{\"r\":\"DEUEL, HANS; SOLMS, JÜRG /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch010 --\"},{\"t\":\"Algin in Review /\"},{\"r\":\"STEINER, ARNOLD B.; McNEELY, WILLIAM H. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch011 --\"},{\"t\":\"Alginates from Common British Brown Marine Algae /\"},{\"r\":\"BLACK, W. A . P.; WOODWARD, F. N. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch012 --\"},{\"t\":\"Irish Moss Extractives /\"},{\"r\":\"STOLOFF, LEONARD /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch013 --\"},{\"t\":\"Effect of Different Ions on Gel Strength Of Red Seaweed Extracts /\"},{\"r\":\"MARSHALL, S. M.; ORR, A. P. /\"},{\"u\":\"http://dx.doi.org/10.1021/ba-1954-0011.ch014\"}],\"ind1\":\"0\",\"ind2\":\"0\"}},{\"506\":{\"subfields\":[{\"a\":\"Online full text is restricted to subscribers.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"583\":{\"subfields\":[{\"a\":\"committed to retain\"},{\"c\":\"20160630\"},{\"d\":\"20310630\"},{\"f\":\"EAST\"},{\"u\":\"https://eastlibraries.org/retained-materials\"},{\"5\":\"DCHS\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"588\":{\"subfields\":[{\"a\":\"Print version record.\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Biocolloids\"},{\"v\":\"Congresses.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"SCIENCE\"},{\"x\":\"Chemistry\"},{\"x\":\"Physical & Theoretical.\"},{\"2\":\"bisacsh\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"650\":{\"subfields\":[{\"a\":\"Biocolloids.\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst00831997\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"650\":{\"subfields\":[{\"a\":\"Colloids\"},{\"x\":\"chemistry.\"}],\"ind1\":\"1\",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Colloids\"},{\"x\":\"economics.\"}],\"ind1\":\"1\",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Pectins\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Agar\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Gum Arabic\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Karaya Gum\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Tragacanth\"},{\"x\":\"history.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Tragacanth\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Plant Gums\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Alginates\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Phaeophyta\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Chondrus\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"650\":{\"subfields\":[{\"a\":\"Rhodophyta\"},{\"x\":\"chemistry.\"}],\"ind1\":\" \",\"ind2\":\"2\"}},{\"655\":{\"subfields\":[{\"a\":\"Electronic books.\"}],\"ind1\":\" \",\"ind2\":\"4\"}},{\"655\":{\"subfields\":[{\"a\":\"Conference papers and proceedings.\"},{\"2\":\"fast\"},{\"0\":\"(OCoLC)fst01423772\"}],\"ind1\":\" \",\"ind2\":\"7\"}},{\"710\":{\"subfields\":[{\"a\":\"American Chemical Society.\"},{\"b\":\"Division of Agricultural and Food Chemistry.\"},{\"0\":\"http://id.loc.gov/authorities/names/n79007703\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"American Chemical Society.\"},{\"b\":\"Division of Colloid and Surface Chemistry.\"},{\"0\":\"http://id.loc.gov/authorities/names/n80109319\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"American Chemical Society.\"},{\"b\":\"Meeting\"},{\"n\":\"(122nd :\"},{\"d\":\"1952 :\"},{\"c\":\"Atlantic City, N.J.)\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"776\":{\"subfields\":[{\"i\":\"Print version:\"},{\"a\":\"American Chemical Society. Division of Colloid and Surface Chemistry.\"},{\"t\":\"Natural plant hydrocolloids.\"},{\"d\":\"Washington, D.C. : American Chemical Society, 1954\"},{\"w\":\"(DLC)   55000367\"},{\"w\":\"(OCoLC)280432\"}],\"ind1\":\"0\",\"ind2\":\"8\"}},{\"830\":{\"subfields\":[{\"a\":\"Advances in chemistry series ;\"},{\"v\":\"11.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"850\":{\"subfields\":[{\"a\":\"AAP\"},{\"a\":\"CU\"},{\"a\":\"DLC\"},{\"a\":\"MiU \"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://silk.library.umass.edu/login?url=https://pubs.acs.org/doi/book/10.1021/ba-1954-0011\"},{\"z\":\"UMass: Link to resource\"}],\"ind1\":\"4\",\"ind2\":\"0\"}},{\"891\":{\"subfields\":[{\"9\":\"853\"},{\"8\":\"1\"},{\"a\":\"(year/year)\"}],\"ind1\":\"3\",\"ind2\":\"3\"}},{\"938\":{\"subfields\":[{\"a\":\"ebrary\"},{\"b\":\"EBRY\"},{\"n\":\"ebr10728707\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    record.getParsedRecord().setContent(incomingParsedContent);
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));

    mappingProfile.getMappingDetails().withMarcMappingOption(MappingDetail.MarcMappingOption.MODIFY);
    profileSnapshotWrapper.getChildSnapshotWrappers().get(0)
      .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
        .withProfileId(marcBibModifyMappingProfile.getId())
        .withContentType(MAPPING_PROFILE)
        .withContent(JsonObject.mapFrom(marcBibModifyMappingProfile).getMap())));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(snapshotForRecordUpdate.getJobExecutionId())
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
