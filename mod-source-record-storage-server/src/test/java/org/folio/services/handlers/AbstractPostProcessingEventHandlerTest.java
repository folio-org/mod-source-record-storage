package org.folio.services.handlers;

import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.MockitoAnnotations;

import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractLBServiceTest;

public abstract class AbstractPostProcessingEventHandlerTest extends AbstractLBServiceTest {

  protected static final String PARSED_CONTENT_WITH_999_FIELD =
    "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";
  protected static final String PARSED_CONTENT_WITHOUT_001_FIELD =
    "{\"leader\":\"01589ccm a2200373   4500\",\"fields\":[{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Neue Ausgabe sämtlicher Werke,\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"bc37566c-0053-4e8b-bd39-15935ca36894\"}]}}]}";
  protected static final String recordId = UUID.randomUUID().toString();
  private static RawRecord rawRecord;
  private static ParsedRecord parsedRecord;
  protected final String snapshotId1 = UUID.randomUUID().toString();
  protected final String snapshotId2 = UUID.randomUUID().toString();
  protected Record record;
  protected RecordDao recordDao;

  protected AbstractPostProcessingEventHandler handler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawRecord = new RawRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(RAW_MARC_RECORD_CONTENT_SAMPLE_PATH), String.class));
    parsedRecord = new ParsedRecord().withId(recordId)
      .withContent(
        new ObjectMapper().readValue(TestUtil.readFileFromPath(PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH), JsonObject.class)
          .encode());
  }

  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.initMocks(this);

    recordDao = new RecordDaoImpl(postgresClientFactory);
    handler = createHandler(recordDao, kafkaConfig);
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
      .withGeneration(0)
      .withRecordType(getMarcType())
      .withRawRecord(rawRecord)
      .withParsedRecord(parsedRecord)
      .withExternalIdsHolder(null);

    SnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), snapshots).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  protected abstract Record.RecordType getMarcType();

  protected abstract AbstractPostProcessingEventHandler createHandler(RecordDao recordDao, KafkaConfig kafkaConfig);

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

  protected DataImportEventPayload createDataImportEventPayload(HashMap<String, String> payloadContext,
                                                                DataImportEventTypes diInventoryInstanceCreatedReadyForPostProcessing) {
    return new DataImportEventPayload()
      .withContext(payloadContext)
      .withEventType(diInventoryInstanceCreatedReadyForPostProcessing.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN);
  }

  protected JsonObject createExternalEntity(String id, String hrid) {
    return new JsonObject()
      .put("id", id)
      .put("hrid", hrid);
  }

  protected String getInventoryId(JsonArray fields) {
    String actualInstanceId = null;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      if (field.containsKey(TAG_999)) {
        JsonArray subfields = field.getJsonObject(TAG_999).getJsonArray("subfields");
        for (int j = 0; j < subfields.size(); j++) {
          JsonObject subfield = subfields.getJsonObject(j);
          if (subfield.containsKey("i")) {
            actualInstanceId = subfield.getString("i");
          }
        }
      }
    }
    return actualInstanceId;
  }

  protected String getInventoryHrid(JsonArray fields) {
    String actualInstanceHrid = null;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject field = fields.getJsonObject(i);
      if (field.containsKey("001")) {
        actualInstanceHrid = field.getString("001");
      }
    }
    return actualInstanceHrid;
  }
}
