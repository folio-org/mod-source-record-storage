package org.folio.services;

import static org.folio.services.util.AdditionalFieldsUtil.TAG_035;
import static org.folio.services.util.AdditionalFieldsUtil.addControlledFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.addDataFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.get035SubfieldOclcValues;
import static org.folio.services.util.AdditionalFieldsUtil.getCacheStats;
import static org.folio.services.util.AdditionalFieldsUtil.getValueFromControlledField;
import static org.folio.services.util.AdditionalFieldsUtil.isFieldExist;
import static org.folio.services.util.AdditionalFieldsUtil.removeField;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.AdditionalFieldsUtil;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.marc4j.marc.Subfield;

@RunWith(BlockJUnit4ClassRunner.class)
public class AdditionalFieldsUtilTest {

  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/parsedMarcRecord.json";

  @Test
  public void shouldAddInstanceIdSubfield() throws IOException {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean addedSourceRecordId = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 's', recordId);
    boolean addedInstanceId = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assert.assertTrue(addedSourceRecordId);
    Assert.assertTrue(addedInstanceId);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    int totalFieldsCount = 0;
    for (int i = fields.size(); i-- > 0; ) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(AdditionalFieldsUtil.TAG_999)) {
        JsonArray subfields = targetField.getJsonObject(AdditionalFieldsUtil.TAG_999).getJsonArray("subfields");
        for (int j = subfields.size(); j-- > 0; ) {
          JsonObject targetSubfield = subfields.getJsonObject(j);
          if (targetSubfield.containsKey("i")) {
            String actualInstanceId = (String) targetSubfield.getValue("i");
            Assert.assertEquals(instanceId, actualInstanceId);
          }
          if (targetSubfield.containsKey("s")) {
            String actualSourceRecordId = (String) targetSubfield.getValue("s");
            Assert.assertEquals(recordId, actualSourceRecordId);
          }
        }
        totalFieldsCount++;
      }
    }
    Assert.assertEquals(2, totalFieldsCount);
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoParsedRecordContent() {
    // given
    Record record = new Record();
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNull(record.getParsedRecord());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfNoFieldsInParsedRecordContent() {
    // given
    Record record = new Record();
    String content = StringUtils.EMPTY;
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    Assert.assertEquals(content, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfCanNotConvertParsedContentToJsonObject() {
    // given
    Record record = new Record();
    String content = "{fields}";
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
    Assert.assertEquals(content, record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfContentHasNoFields() {
    // given
    Record record = new Record();
    String content = "{\"leader\":\"01240cas a2200397\"}";
    record.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNotNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldNotAddInstanceIdSubfieldIfContentIsNull() {
    // given
    Record record = new Record();
    record.setParsedRecord(new ParsedRecord().withContent(null));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assert.assertFalse(added);
    Assert.assertNotNull(record.getParsedRecord());
    Assert.assertNull(record.getParsedRecord().getContent());
  }

  @Test
  public void shouldRemoveField() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean deleted = removeField(record, "001");
    Assert.assertTrue(deleted);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("001")) {
        Assert.fail();
      }
    }
  }

  @Test
  public void shouldAddControlledFieldToMarcRecord() throws IOException {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = AdditionalFieldsUtil.addControlledFieldToMarcRecord(record, "002", "test");
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean passed = false;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("002") && targetField.getString("002").equals("test")) {
        passed = true;
        break;
      }
    }
    Assert.assertTrue(passed);
  }

  @Test
  public void shouldAddFieldToMarcRecordInNumericalOrder() throws IOException {
    // given
    String instanceHrId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceHrId);
    // then
    Assert.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assert.assertNotEquals(leader, newLeader);
    Assert.assertFalse(fields.isEmpty());
    boolean existsNewField = false;
    for (int i = 0; i < fields.size() - 1; i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("035")) {
        existsNewField = true;
        String currentTag = fields.getJsonObject(i).stream().map(Map.Entry::getKey).findFirst().get();
        String nextTag = fields.getJsonObject(i + 1).stream().map(Map.Entry::getKey).findFirst().get();
        MatcherAssert.assertThat(currentTag, lessThanOrEqualTo(nextTag));
      }
    }
    Assert.assertTrue(existsNewField);
  }


  @Test
  public void shouldNotSortExistingFieldsWhenAddFieldToToMarcRecord() {
    // given
    String instanceId = "12345";
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00113nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"i\":\"12345\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(record, "999", 'f', 'f', 'i', instanceId);
    // then
    Assert.assertTrue(added);
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotAdd035AndAdd001FieldsIf001And003FieldsNotExists() {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotAdd035if001containsHRID() {
    // given
    String parsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldRemove003ifHRIDManipulationAlreadyDone() {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldRemove035ifItContainsHRID() {
    // given
    String parsedContent = "{\"leader\":\"00118nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(record, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldReturnSubfieldIfOclcExist() {
    // given
    String parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
      "{\"a\":\"(OCoLC)64758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var expectedSubfields =  List.of("(OCoLC)64758");

    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    var subfields = get035SubfieldOclcValues(record, TAG_035).stream().map(Subfield::getData).toList();
    // then
    Assert.assertEquals(expectedSubfields.size(), subfields.size());
    Assert.assertEquals(expectedSubfields.getFirst(), subfields.getFirst());
  }

  @Test
  public void shouldRemovePeriodsAndSpacesAfterNormalization() {
    // given
    var parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)on. 607TST .001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    var expectedParsedContent = "{\"leader\":\"00098nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(record);
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldPreserveOrderOf035FieldsAfterNormalization() {
    // given
    var parsedContent = "{\"leader\":\"00198cama 22003611a 4500\",\"fields\":[" +
      "{\"001\":\"10065352\"}," +
      "{\"005\":\"20220127143948.0\"}," +
      "{\"008\":\"761216s1853mauch0010eng\"}," +
      "{\"906\":{\"subfields\":[{\"a\":\"7\"},{\"b\":\"cbc\"},{\"c\":\"oclcrpl\"},{\"d\":\"u\"},{\"e\":\"ncip\"},{\"f\":\"19\"},{\"g\":\"y-gencatlg\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
      "{\"035\":{\"subfields\":[{\"9\":\"(DLC)01012052\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
      "{\"010\":{\"subfields\":[{\"a\":\"01012052\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)2628488\"}],\"ind1\":\"\",\"ind2\":\"\"}}," +
      "{\"040\":{\"subfields\":[{\"a\":\"DLC\"},{\"b\":\"eng\"},{\"c\":\"O\"},{\"d\":\"O\"},{\"d\":\"DLC\"}],\"ind1\":\"\",\"ind2\":\"\"}}]}";

    var expectedParsedContent = "{\"leader\":\"00291cama 22001211a 4500\",\"fields\":[" +
      "{\"001\":\"10065352\"}," +
      "{\"005\":\"20220127143948.0\"}," +
      "{\"008\":\"761216s1853mauch0010eng\"}," +
      "{\"906\":{\"subfields\":[{\"a\":\"7\"},{\"b\":\"cbc\"},{\"c\":\"oclcrpl\"},{\"d\":\"u\"},{\"e\":\"ncip\"},{\"f\":\"19\"},{\"g\":\"y-gencatlg\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"035\":{\"subfields\":[{\"9\":\"(DLC)01012052\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"010\":{\"subfields\":[{\"a\":\"01012052\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)2628488\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"040\":{\"subfields\":[{\"a\":\"DLC\"},{\"b\":\"eng\"},{\"c\":\"O\"},{\"d\":\"O\"},{\"d\":\"DLC\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(record);
    Assert.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  public void shouldNotReturnSubfieldIfOclcNotExist() {
    // given
    String parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    var subfields = get035SubfieldOclcValues(record, TAG_035).stream().map(Subfield::getData).toList();
    // then
    Assert.assertEquals(0, subfields.size());
  }

  @Test
  public void caching() throws IOException {
    // given
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();

    CacheStats initialCacheStats = getCacheStats();

    // record with null parsed content
    Assert.assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString()), "035", 'a', instanceId));
    CacheStats cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(0, cacheStats.missCount());
    Assert.assertEquals(0, cacheStats.loadCount());
    // record with empty parsed content
    Assert.assertFalse(
      isFieldExist(
        new Record()
          .withId(UUID.randomUUID().toString())
          .withParsedRecord(new ParsedRecord().withContent("")),
        "035",
        'a',
        instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(0, cacheStats.requestCount());
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(0, cacheStats.missCount());
    Assert.assertEquals(0, cacheStats.loadCount());
    // record with bad parsed content
    Assert.assertFalse(
      isFieldExist(
        new Record()
          .withId(UUID.randomUUID().toString())
          .withParsedRecord(new ParsedRecord().withContent("test")),
        "035",
        'a',
        instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(1, cacheStats.requestCount());
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(1, cacheStats.missCount());
    Assert.assertEquals(1, cacheStats.loadCount());
    // does field exists?
    Assert.assertFalse(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(2, cacheStats.requestCount());
    Assert.assertEquals(0, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // update field
    addDataFieldToMarcRecord(record, "035", ' ', ' ', 'a', instanceId);
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(3, cacheStats.requestCount());
    Assert.assertEquals(1, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // verify that field exists
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(4, cacheStats.requestCount());
    Assert.assertEquals(2, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // verify that field exists again
    Assert.assertTrue(isFieldExist(record, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(5, cacheStats.requestCount());
    Assert.assertEquals(3, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // remove the field
    Assert.assertTrue(removeField(record, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(6, cacheStats.requestCount());
    Assert.assertEquals(4, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // get value from controlled field
    Assert.assertEquals(getValueFromControlledField(record,"001"),"ybp7406411");
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(7, cacheStats.requestCount());
    Assert.assertEquals(5, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // add controlled field to marc record
    Assert.assertTrue(addControlledFieldToMarcRecord(record, "002", "test"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(8, cacheStats.requestCount());
    Assert.assertEquals(6, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
    // add field to marc record
    Assert.assertTrue(addFieldToMarcRecord(record, AdditionalFieldsUtil.TAG_999, 'i', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assert.assertEquals(9, cacheStats.requestCount());
    Assert.assertEquals(7, cacheStats.hitCount());
    Assert.assertEquals(2, cacheStats.missCount());
    Assert.assertEquals(2, cacheStats.loadCount());
  }

  @Test
  public void isFieldsFillingNeededTrue() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId)
        .withInstanceHrid(UUID.randomUUID().toString()))
      .withRecordType(Record.RecordType.MARC_BIB);

    JsonObject instanceJson = new JsonObject();
    instanceJson.put("id", instanceId);
    instanceJson.put("hrid", instanceHrId);

    Assert.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));

    srcRecord.getExternalIdsHolder().setInstanceHrid(null);
    Assert.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));
  }

  @Test
  public void isFieldsFillingNeededFalse() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId)
        .withInstanceHrid(instanceHrId))
      .withRecordType(Record.RecordType.MARC_BIB);

    JsonObject instanceJson = new JsonObject();
    instanceJson.put("id", instanceId);
    instanceJson.put("hrid", instanceHrId);

    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));

    srcRecord.getExternalIdsHolder().withInstanceId(instanceId);
    instanceJson.put("id", UUID.randomUUID().toString());
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));

    srcRecord.getExternalIdsHolder().withInstanceId(null).withInstanceHrid(null);
    assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));
  }

  @Test(expected = Exception.class)
  public void isFieldsFillingNeededForExternalHolderInstanceShouldThrowException() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(instanceId)
      .withInstanceHrid(instanceHrId))
      .withRecordType(Record.RecordType.MARC_BIB);

    JsonObject instanceJson = new JsonObject();
    instanceJson.put("hrid", instanceHrId);
    AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson);
  }

  @Test
  public void isFieldsFillingNeededForHoldingsExternalHolder() {
    String holdingId = UUID.randomUUID().toString();
    String holdingHrid = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingId))
      .withRecordType(Record.RecordType.MARC_HOLDING);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put("id", holdingId);
    jsonObject.put("hrid", holdingHrid);

    Assert.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));

    srcRecord.getExternalIdsHolder().setHoldingsHrid(holdingHrid);

    Assert.assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));
  }

  @Test
  public void isFieldsFillingNeededForUnknownRecordType() {
    String entityId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withRecordType(any());

    JsonObject jsonObject = new JsonObject();
    jsonObject.put("id", entityId);

    Assert.assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));
  }

  @Test
  public void isFieldsFillingNeededTrueForMarcAuthority() {
    String authorityId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(authorityId))
      .withRecordType(Record.RecordType.MARC_AUTHORITY);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put("id", authorityId);

    Assert.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));
  }
}
