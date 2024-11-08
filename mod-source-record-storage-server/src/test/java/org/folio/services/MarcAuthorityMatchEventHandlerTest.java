package org.folio.services;

import static java.util.Collections.singletonList;
import static org.folio.MatchDetail.MatchCriterion.EXACTLY_MATCHES;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.MatchExpression.DataValueType.VALUE_FROM_RECORD;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.services.handlers.match.AbstractMarcMatchEventHandler.MULTI_MATCH_IDS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.ReactToType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.domainevent.RecordDomainEventPublisher;
import org.folio.services.handlers.match.MarcAuthorityMatchEventHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class MarcAuthorityMatchEventHandlerTest extends AbstractLBServiceTest {

  private static final String PARSED_CONTENT = "{ \"leader\": \"01012cz  a2200241n  4500\", \"fields\": [ { \"001\": \"1000649\" }, { \"005\": \"20171119085041.0\" }, { \"008\": \"201001 n acanaaabn           n aaa     d\" }, { \"010\": { \"subfields\": [ { \"a\": \"n   58020553 \" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"024\": { \"subfields\": [ { \"a\": \"0022-0469\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"035\": { \"subfields\": [ { \"a\": \"90c37ff4-2f1e-451f-8822-87241b081617\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"100\": { \"subfields\": [ { \"a\": \"Eimermacher, Karl\" }, { \"d\": \"CtY\" }, { \"d\": \"MBTI\" }, { \"d\": \"CtY\" }, { \"d\": \"MBTI\" }, { \"d\": \"NIC\" }, { \"d\": \"CStRLIN\" }, { \"d\": \"NIC\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"110\": { \"subfields\": [ { \"a\": \"BR140\" }, { \"b\": \".J6\" } ], \"ind1\": \"0\", \"ind2\": \" \" } }, { \"111\": { \"subfields\": [ { \"a\": \"270.05\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"130\": { \"subfields\": [ { \"a\": \"The Journal of ecclesiastical history\" } ], \"ind1\": \"0\", \"ind2\": \"4\" } }, { \"150\": { \"subfields\": [ { \"a\": \"The Journal of ecclesiastical history.\" } ], \"ind1\": \"0\", \"ind2\": \"4\" } }, { \"151\": { \"subfields\": [ { \"a\": \"London,\" }, { \"b\": \"Cambridge University Press [etc.]\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"155\": { \"subfields\": [ { \"a\": \"32 East 57th St., New York, 10022\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"375\": { \"subfields\": [ { \"a\": \"male\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"377\": { \"subfields\": [ { \"a\": \"ger\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"400\": { \"subfields\": [ { \"a\": \"v.\" }, { \"b\": \"25 cm.\" } ], \"ind1\": \"1\", \"ind2\": \" \" } }, { \"410\": { \"subfields\": [ { \"a\": \"Quarterly,\" }, { \"b\": \"1970-\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"411\": { \"subfields\": [ { \"a\": \"Semiannual,\" }, { \"b\": \"1950-69\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"430\": { \"subfields\": [ { \"a\": \"v. 1-   Apr. 1950-\" } ], \"ind1\": \"0\", \"ind2\": \" \" } }, { \"450\": { \"subfields\": [ { \"a\": \"note$a\" }, { \"u\": \"note$u\" }, { \"3\": \"note$3\" }, { \"5\": \"note$5\" }, { \"6\": \"note$6\" }, { \"8\": \"note$8\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"451\": { \"subfields\": [ { \"a\": \"note$a\" }, { \"b\": \"note$b\" }, { \"c\": \"note$c\" }, { \"d\": \"note$d\" }, { \"e\": \"note$e\" }, { \"3\": \"note$3\" }, { \"5\": \"note$5\" }, { \"6\": \"note$6\" }, { \"8\": \"note$8\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"455\": { \"subfields\": [ { \"a\": \"note$a\" }, { \"b\": \"note$b\" }, { \"c\": \"note$c\" }, { \"d\": \"note$d\" }, { \"e\": \"note$e\" }, { \"f\": \"note$f\" }, { \"h\": \"note$h\" }, { \"i\": \"note$i\" }, { \"j\": \"note$j\" }, { \"k\": \"note$k\" }, { \"l\": \"note$l\" }, { \"n\": \"note$n\" }, { \"o\": \"note$o\" }, { \"u\": \"note$u\" }, { \"x\": \"note$x\" }, { \"z\": \"note$z\" }, { \"2\": \"note$2\" }, { \"3\": \"note$3\" }, { \"5\": \"note$5\" }, { \"8\": \"note$8\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"500\": { \"subfields\": [ { \"a\": \"Editor:   C. W. Dugmore.\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"510\": { \"subfields\": [ { \"a\": \"Church history\" }, { \"x\": \"Periodicals.\" } ], \"ind1\": \" \", \"ind2\": \"0\" } }, { \"511\": { \"subfields\": [ { \"a\": \"Church history\" }, { \"2\": \"fast\" }, { \"0\": \"(OCoLC)fst00860740\" } ], \"ind1\": \" \", \"ind2\": \"7\" } }, { \"530\": { \"subfields\": [ { \"a\": \"Periodicals\" }, { \"2\": \"fast\" }, { \"0\": \"(OCoLC)fst01411641\" } ], \"ind1\": \" \", \"ind2\": \"7\" } }, { \"550\": { \"subfields\": [ { \"a\": \"Dugmore, C. W.\" }, { \"q\": \"(Clifford William),\" }, { \"e\": \"ed.\" } ], \"ind1\": \"1\", \"ind2\": \" \" } }, { \"551\": { \"subfields\": [ { \"k\": \"callNumberPrefix\" }, { \"h\": \"callNumber1\" }, { \"i\": \"callNumber2\" }, { \"m\": \"callNumberSuffix\" }, { \"t\": \"copyNumber\" } ], \"ind1\": \"0\", \"ind2\": \"3\" } }, { \"555\": { \"subfields\": [ { \"u\": \"uri\" }, { \"y\": \"linkText\" }, { \"3\": \"materialsSpecification\" }, { \"z\": \"publicNote\" } ], \"ind1\": \"0\", \"ind2\": \"3\" } }, { \"999\": { \"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [ { \"s\": \"b90cb1bc-601f-45d7-b99e-b11efd281dcd\" } ] } } ] }";
  private static final String PARSED_CONTENT2 = "{ \"leader\": \"01012cz  a2200241n  4500\", \"fields\": [ { \"001\": \"1000649\" }, { \"005\": \"20171119085041.0\" }, { \"008\": \"201001 n acanaaabn           n aaa     d\" }, { \"010\": { \"subfields\": [ { \"a\": \"n   7777777 \" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"024\": { \"subfields\": [ { \"a\": \"0022-0469\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"035\": { \"subfields\": [ { \"a\": \"90c37ff4-2f1e-451f-8822-87241b081617\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"100\": { \"subfields\": [ { \"a\": \"Eimermacher, Karl\" }, { \"d\": \"CtY\" }, { \"d\": \"MBTI\" }, { \"d\": \"CtY\" }, { \"d\": \"MBTI\" }, { \"d\": \"NIC\" }, { \"d\": \"CStRLIN\" }, { \"d\": \"NIC\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"110\": { \"subfields\": [ { \"a\": \"BR140\" }, { \"b\": \".J6\" } ], \"ind1\": \"0\", \"ind2\": \" \" } }, { \"111\": { \"subfields\": [ { \"a\": \"270.05\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"130\": { \"subfields\": [ { \"a\": \"The Journal of ecclesiastical history\" } ], \"ind1\": \"0\", \"ind2\": \"4\" } }, { \"150\": { \"subfields\": [ { \"a\": \"The Journal of ecclesiastical history.\" } ], \"ind1\": \"0\", \"ind2\": \"4\" } }, { \"151\": { \"subfields\": [ { \"a\": \"London,\" }, { \"b\": \"Cambridge University Press [etc.]\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"155\": { \"subfields\": [ { \"a\": \"32 East 57th St., New York, 10022\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"375\": { \"subfields\": [ { \"a\": \"male\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"377\": { \"subfields\": [ { \"a\": \"ger\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"400\": { \"subfields\": [ { \"a\": \"v.\" }, { \"b\": \"25 cm.\" } ], \"ind1\": \"1\", \"ind2\": \" \" } }, { \"410\": { \"subfields\": [ { \"a\": \"Quarterly,\" }, { \"b\": \"1970-\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"411\": { \"subfields\": [ { \"a\": \"Semiannual,\" }, { \"b\": \"1950-69\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"430\": { \"subfields\": [ { \"a\": \"v. 1-   Apr. 1950-\" } ], \"ind1\": \"0\", \"ind2\": \" \" } }, { \"450\": { \"subfields\": [ { \"a\": \"note$a\" }, { \"u\": \"note$u\" }, { \"3\": \"note$3\" }, { \"5\": \"note$5\" }, { \"6\": \"note$6\" }, { \"8\": \"note$8\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"451\": { \"subfields\": [ { \"a\": \"note$a\" }, { \"b\": \"note$b\" }, { \"c\": \"note$c\" }, { \"d\": \"note$d\" }, { \"e\": \"note$e\" }, { \"3\": \"note$3\" }, { \"5\": \"note$5\" }, { \"6\": \"note$6\" }, { \"8\": \"note$8\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"455\": { \"subfields\": [ { \"a\": \"note$a\" }, { \"b\": \"note$b\" }, { \"c\": \"note$c\" }, { \"d\": \"note$d\" }, { \"e\": \"note$e\" }, { \"f\": \"note$f\" }, { \"h\": \"note$h\" }, { \"i\": \"note$i\" }, { \"j\": \"note$j\" }, { \"k\": \"note$k\" }, { \"l\": \"note$l\" }, { \"n\": \"note$n\" }, { \"o\": \"note$o\" }, { \"u\": \"note$u\" }, { \"x\": \"note$x\" }, { \"z\": \"note$z\" }, { \"2\": \"note$2\" }, { \"3\": \"note$3\" }, { \"5\": \"note$5\" }, { \"8\": \"note$8\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"500\": { \"subfields\": [ { \"a\": \"Editor:   C. W. Dugmore.\" } ], \"ind1\": \" \", \"ind2\": \" \" } }, { \"510\": { \"subfields\": [ { \"a\": \"Church history\" }, { \"x\": \"Periodicals.\" } ], \"ind1\": \" \", \"ind2\": \"0\" } }, { \"511\": { \"subfields\": [ { \"a\": \"Church history\" }, { \"2\": \"fast\" }, { \"0\": \"(OCoLC)fst00860740\" } ], \"ind1\": \" \", \"ind2\": \"7\" } }, { \"530\": { \"subfields\": [ { \"a\": \"Periodicals\" }, { \"2\": \"fast\" }, { \"0\": \"(OCoLC)fst01411641\" } ], \"ind1\": \" \", \"ind2\": \"7\" } }, { \"550\": { \"subfields\": [ { \"a\": \"Dugmore, C. W.\" }, { \"q\": \"(Clifford William),\" }, { \"e\": \"ed.\" } ], \"ind1\": \"1\", \"ind2\": \" \" } }, { \"551\": { \"subfields\": [ { \"k\": \"callNumberPrefix\" }, { \"h\": \"callNumber1\" }, { \"i\": \"callNumber2\" }, { \"m\": \"callNumberSuffix\" }, { \"t\": \"copyNumber\" } ], \"ind1\": \"0\", \"ind2\": \"3\" } }, { \"555\": { \"subfields\": [ { \"u\": \"uri\" }, { \"y\": \"linkText\" }, { \"3\": \"materialsSpecification\" }, { \"z\": \"publicNote\" } ], \"ind1\": \"0\", \"ind2\": \"3\" } }, { \"999\": { \"ind1\": \"f\", \"ind2\": \"f\", \"subfields\": [ { \"s\": \"b90cb1bc-601f-45d7-b99e-b11efd281dcd\" } ] } } ] }";
  private static final String MATCHED_MARC_KEY = "MATCHED_MARC_AUTHORITY";
  private static final String existingRecordId = "b90cb1bc-601f-45d7-b99e-b11efd281dcd";
  private static final String existingRecord2Id = UUID.randomUUID().toString();
  private static String rawRecordContent;
  @Mock
  private RecordDomainEventPublisher recordDomainEventPublisher;
  private RecordDao recordDao;
  private Record existingRecord;
  private Record existingRecord2;
  private Record incomingRecord;
  private Record incomingRecord2;
  private Snapshot existingRecordSnapshot;
  private EventHandler handler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecordContent = new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class);
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);

    recordDao = new RecordDaoImpl(postgresClientFactory, recordDomainEventPublisher);
    handler = new MarcAuthorityMatchEventHandler(recordDao, null, vertx);
    Async async = context.async();

    existingRecordSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);
    Snapshot incomingRecordSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date())
      .withStatus(Snapshot.Status.COMMITTED);

    List<Snapshot> snapshots = new ArrayList<>();
    snapshots.add(existingRecordSnapshot);
    snapshots.add(incomingRecordSnapshot);

    this.existingRecord = new Record()
      .withId(existingRecordId)
      .withMatchedId(existingRecordId)
      .withSnapshotId(existingRecordSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(existingRecordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(existingRecordId).withContent(PARSED_CONTENT))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityHrid("1000649")
      )
      .withState(Record.State.ACTUAL);

    this.existingRecord2 = new Record()
      .withId(existingRecord2Id)
      .withMatchedId(existingRecord2Id)
      .withSnapshotId(existingRecordSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(existingRecord2Id).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(existingRecord2Id).withContent(PARSED_CONTENT2))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityHrid("1000650")
      )
      .withState(Record.State.ACTUAL);

    String incomingRecordId = UUID.randomUUID().toString();
    this.incomingRecord = new Record()
      .withId(incomingRecordId)
      .withMatchedId(existingRecord.getId())
      .withSnapshotId(incomingRecordSnapshot.getJobExecutionId())
      .withGeneration(1)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(incomingRecordId).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(incomingRecordId).withContent(PARSED_CONTENT))
      .withExternalIdsHolder(new ExternalIdsHolder());

    String incomingRecord2Id = UUID.randomUUID().toString();
    this.incomingRecord2 = new Record()
      .withId(incomingRecord2Id)
      .withMatchedId(existingRecord.getId())
      .withSnapshotId(incomingRecordSnapshot.getJobExecutionId())
      .withGeneration(1)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(incomingRecord2Id).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(incomingRecord2Id).withContent(PARSED_CONTENT2))
      .withExternalIdsHolder(new ExternalIdsHolder());

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
  public void shouldMatchBy999ffsField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(incomingRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("s")
              )))
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("s")
              )))
          ))));

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(existingSavedRecord -> handler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value());
          context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_KEY)).mapTo(Record.class), existingSavedRecord);
          async.complete();
        }));
  }

  @Test
  public void shouldMatchBy001Field(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(existingRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("001"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("")
              )))
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("001"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("")
              )))))));

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(existingSavedRecord -> handler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value());
          context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_KEY)).mapTo(Record.class), existingSavedRecord);
          async.complete();
        }));
  }

  @Test
  public void shouldMatchBy010aField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(incomingRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("010"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a")
              )))
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("010"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a")
              )))
          ))));

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(existingSavedRecord -> handler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value());
          context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_KEY)).mapTo(Record.class), existingSavedRecord);
          async.complete();
        }));
  }

  @Test
  public void shouldMatchMultipleRecordsIfMatchProfileIsNextInProfileSnapshot(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(incomingRecord2));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withReactTo(ReactToType.MATCH)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("010"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a")
              )))
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("010"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a")
              )))
          ))).withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper().withId(UUID.randomUUID().toString())
          .withContentType(MATCH_PROFILE)
          .withReactTo(ReactToType.MATCH)
          .withContent(new MatchProfile()
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)))));

    String existingRecord3Id = UUID.randomUUID().toString();
    Record existingRecord3 = new Record()
      .withId(existingRecord3Id)
      .withMatchedId(existingRecord3Id)
      .withSnapshotId(existingRecordSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(existingRecord3Id).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(existingRecord3Id).withContent(PARSED_CONTENT2))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityHrid("1000650")
      )
      .withState(Record.State.ACTUAL);

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord2, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(existingSavedRecord -> recordDao.saveRecord(existingRecord3, okapiHeaders)
        .onComplete(context.asyncAssertSuccess())
        .onSuccess(existingSavedRecord2 -> handler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value());
          context.assertTrue(updatedEventPayload.getContext().containsKey(MULTI_MATCH_IDS));
          List<String> multiIds = new JsonArray(updatedEventPayload.getContext().get(MULTI_MATCH_IDS))
            .stream().map(o -> (String) o)
            .toList();
          context.assertEquals(multiIds.size(), 2);
          context.assertTrue(multiIds.contains(existingRecord2.getId()));
          context.assertTrue(multiIds.contains(existingRecord3.getId()));
          async.complete();
        })));
  }

  @Test
  public void shouldNarrowMultipleMatchedRecordsDuringSubmatch(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(incomingRecord2));
    payloadContext.put(MULTI_MATCH_IDS, JsonArray.of(existingRecordId, existingRecord2Id).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("010"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a")
              )))
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("010"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a")
              )))
          ))));

    String existingRecord3Id = UUID.randomUUID().toString();
    Record existingRecord3 = new Record()
      .withId(existingRecord3Id)
      .withMatchedId(existingRecord3Id)
      .withSnapshotId(existingRecordSnapshot.getJobExecutionId())
      .withGeneration(0)
      .withRecordType(MARC_AUTHORITY)
      .withRawRecord(new RawRecord().withId(existingRecord3Id).withContent(rawRecordContent))
      .withParsedRecord(new ParsedRecord().withId(existingRecord3Id).withContent(PARSED_CONTENT2))
      .withExternalIdsHolder(new ExternalIdsHolder()
        .withAuthorityHrid("1000650")
      )
      .withState(Record.State.ACTUAL);

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(existingSavedRecord -> recordDao.saveRecord(existingRecord2, okapiHeaders)
        .onComplete(context.asyncAssertSuccess())
        .onSuccess(existingSavedRecord2 -> recordDao.saveRecord(existingRecord3, okapiHeaders)
          .onComplete(context.asyncAssertSuccess())
          .onSuccess(existingSavedRecord3 -> handler.handle(dataImportEventPayload)
            .whenComplete((updatedEventPayload, throwable) -> {
              context.assertNull(throwable);
              context.assertEquals(1, updatedEventPayload.getEventsChain().size());
              context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_MATCHED.value());
              context.assertEquals(new JsonObject(updatedEventPayload.getContext().get(MATCHED_MARC_KEY)).mapTo(Record.class), existingSavedRecord2);
              async.complete();
            }))));
  }

  @Test
  public void shouldNotMatchBy999ffsField(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(existingRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("999"),
                new Field().withLabel("indicator1").withValue("f"),
                new Field().withLabel("indicator2").withValue("f"),
                new Field().withLabel("recordSubfield").withValue("s"))))
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("035"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a"))))))));

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(existingSavedRecord -> handler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED.value());
          context.assertNull(updatedEventPayload.getContext().get(MATCHED_MARC_KEY));
          async.complete();
        }));
  }

  @Test
  public void shouldNotMatchBy001Field(TestContext context) {
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_AUTHORITY.value(), Json.encode(existingRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(MATCH_PROFILE)
        .withContent(new MatchProfile()
          .withExistingRecordType(EntityType.MARC_AUTHORITY)
          .withIncomingRecordType(EntityType.MARC_AUTHORITY)
          .withMatchDetails(singletonList(new MatchDetail()
            .withMatchCriterion(EXACTLY_MATCHES)
            .withExistingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("001")
              )))
            .withExistingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingRecordType(EntityType.MARC_AUTHORITY)
            .withIncomingMatchExpression(new MatchExpression()
              .withDataValueType(VALUE_FROM_RECORD)
              .withFields(Lists.newArrayList(
                new Field().withLabel("field").withValue("035"),
                new Field().withLabel("indicator1").withValue(""),
                new Field().withLabel("indicator2").withValue(""),
                new Field().withLabel("recordSubfield").withValue("a"))))))));

    var okapiHeaders = Map.of(OKAPI_TENANT_HEADER, TENANT_ID);
    recordDao.saveRecord(existingRecord, okapiHeaders)
      .onComplete(context.asyncAssertSuccess())
      .onSuccess(record -> handler.handle(dataImportEventPayload)
        .whenComplete((updatedEventPayload, throwable) -> {
          context.assertNull(throwable);
          context.assertEquals(1, updatedEventPayload.getEventsChain().size());
          context.assertEquals(updatedEventPayload.getEventType(), DI_SRS_MARC_AUTHORITY_RECORD_NOT_MATCHED.value());
          context.assertNull(updatedEventPayload.getContext().get(MATCHED_MARC_KEY));
          async.complete();
        }));
  }

  @Test
  public void shouldReturnTrueWhenHandlerIsEligibleForProfile() {
    MatchProfile matchProfile = new MatchProfile()
      .withId(UUID.randomUUID().toString())
      .withName("MARC-MARC matching")
      .withIncomingRecordType(EntityType.MARC_AUTHORITY)
      .withExistingRecordType(EntityType.MARC_AUTHORITY);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withContent(matchProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.toString())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertTrue(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenHandlerIsNotEligibleForProfile() {
    MatchProfile matchProfile = new MatchProfile()
      .withId(UUID.randomUUID().toString())
      .withName("MARC-MARC matching")
      .withIncomingRecordType(EntityType.MARC_AUTHORITY)
      .withExistingRecordType(EntityType.HOLDINGS);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(matchProfile.getId())
      .withContentType(MATCH_PROFILE)
      .withContent(matchProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.toString())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }

  @Test
  public void shouldReturnFalseWhenNotMatchProfileForProfile() {
    MappingProfile mappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create authority")
      .withIncomingRecordType(EntityType.MARC_AUTHORITY)
      .withExistingRecordType(EntityType.AUTHORITY);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(mappingProfile.getId())
      .withContentType(MAPPING_PROFILE)
      .withContent(mappingProfile);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withTenant(TENANT_ID)
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.toString())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper);

    boolean isEligible = handler.isEligible(dataImportEventPayload);

    Assert.assertFalse(isEligible);
  }
}
