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
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.TestUtil;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.AdditionalFieldsUtil;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.marc4j.marc.Subfield;

public class AdditionalFieldsUtilTest {

  private static final String PARSED_MARC_RECORD_PATH = "src/test/resources/parsedMarcRecord.json";

  @Test
  void shouldAddInstanceIdSubfield() {
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();

    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    boolean addedSourceRecordId = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 's', recordId);
    boolean addedInstanceId = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assertions.assertTrue(addedSourceRecordId);
    Assertions.assertTrue(addedInstanceId);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assertions.assertNotEquals(leader, newLeader);
    Assertions.assertFalse(fields.isEmpty());
    int totalFieldsCount = 0;
    for (int i = fields.size(); i-- > 0; ) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey(AdditionalFieldsUtil.TAG_999)) {
        JsonArray subfields = targetField.getJsonObject(AdditionalFieldsUtil.TAG_999).getJsonArray("subfields");
        for (int j = subfields.size(); j-- > 0; ) {
          JsonObject targetSubfield = subfields.getJsonObject(j);
          if (targetSubfield.containsKey("i")) {
            String actualInstanceId = (String) targetSubfield.getValue("i");
            Assertions.assertEquals(instanceId, actualInstanceId);
          }
          if (targetSubfield.containsKey("s")) {
            String actualSourceRecordId = (String) targetSubfield.getValue("s");
            Assertions.assertEquals(recordId, actualSourceRecordId);
          }
        }
        totalFieldsCount++;
      }
    }
    Assertions.assertEquals(2, totalFieldsCount);
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfNoParsedRecordContent() {
    // given
    Record marcRecord = new Record();
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assertions.assertFalse(added);
    Assertions.assertNull(marcRecord.getParsedRecord());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfNoFieldsInParsedRecordContent() {
    // given
    Record marcRecord = new Record();
    String content = StringUtils.EMPTY;
    marcRecord.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assertions.assertFalse(added);
    Assertions.assertNotNull(marcRecord.getParsedRecord());
    Assertions.assertNotNull(marcRecord.getParsedRecord().getContent());
    Assertions.assertEquals(content, marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfCanNotConvertParsedContentToJsonObject() {
    // given
    Record marcRecord = new Record();
    String content = "{fields}";
    marcRecord.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assertions.assertFalse(added);
    Assertions.assertNotNull(marcRecord.getParsedRecord());
    Assertions.assertNotNull(marcRecord.getParsedRecord().getContent());
    Assertions.assertEquals(content, marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfContentHasNoFields() {
    // given
    Record marcRecord = new Record();
    String content = "{\"leader\":\"01240cas a2200397\"}";
    marcRecord.setParsedRecord(new ParsedRecord().withContent(content));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assertions.assertFalse(added);
    Assertions.assertNotNull(marcRecord.getParsedRecord());
    Assertions.assertNotNull(marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldNotAddInstanceIdSubfieldIfContentIsNull() {
    // given
    Record marcRecord = new Record();
    marcRecord.setParsedRecord(new ParsedRecord().withContent(null));
    String instanceId = UUID.randomUUID().toString();
    // when
    boolean added = AdditionalFieldsUtil.addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId);
    // then
    Assertions.assertFalse(added);
    Assertions.assertNotNull(marcRecord.getParsedRecord());
    Assertions.assertNull(marcRecord.getParsedRecord().getContent());
  }

  @Test
  void shouldRemoveField() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean deleted = removeField(marcRecord, "001");
    Assertions.assertTrue(deleted);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assertions.assertNotEquals(leader, newLeader);
    Assertions.assertFalse(fields.isEmpty());
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("001")) {
        Assertions.fail();
      }
    }
  }

  @Test
  void shouldAddControlledFieldToMarcRecord() {
    String recordId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record marcRecord = new Record().withId(recordId).withParsedRecord(parsedRecord);
    boolean added = AdditionalFieldsUtil.addControlledFieldToMarcRecord(marcRecord, "002", "test");
    Assertions.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assertions.assertNotEquals(leader, newLeader);
    Assertions.assertFalse(fields.isEmpty());
    boolean passed = false;
    for (int i = 0; i < fields.size(); i++) {
      JsonObject targetField = fields.getJsonObject(i);
      if (targetField.containsKey("002") && targetField.getString("002").equals("test")) {
        passed = true;
        break;
      }
    }
    Assertions.assertTrue(passed);
  }

  @Test
  void shouldAddFieldToMarcRecordInNumericalOrder() {
    // given
    String instanceHrId = UUID.randomUUID().toString();
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    String leader = new JsonObject(parsedRecordContent).getString("leader");
    parsedRecord.setContent(parsedRecordContent);
    Record marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(marcRecord, "035", ' ', ' ', 'a', instanceHrId);
    // then
    Assertions.assertTrue(added);
    JsonObject content = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = content.getJsonArray("fields");
    String newLeader = content.getString("leader");
    Assertions.assertNotEquals(leader, newLeader);
    Assertions.assertFalse(fields.isEmpty());
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
    Assertions.assertTrue(existsNewField);
  }


  @Test
  void shouldNotSortExistingFieldsWhenAddFieldToToMarcRecord() {
    // given
    String instanceId = "12345";
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String expectedParsedContent = "{\"leader\":\"00113nam  22000731a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"999\":{\"subfields\":[{\"i\":\"12345\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);
    Record marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    // when
    boolean added = addDataFieldToMarcRecord(marcRecord, "999", 'f', 'f', 'i', instanceId);
    // then
    Assertions.assertTrue(added);
    Assertions.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  private static Stream<Arguments> fillHrIdFieldArguments() {
    return Stream.of(
      // 001 and 003 fields not exist: 003 renamed to 001
      Arguments.of("{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"),
      // 001 already contains HRID: content unchanged
      Arguments.of("{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"),
      // 003 present after HRID manipulation already done: 003 removed
      Arguments.of("{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}"),
      // 035 contains HRID: 035 (and 003) removed
      Arguments.of("{\"leader\":\"00118nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"},{\"003\":\"qwerty\"},{\"035\":{\"subfields\":[{\"a\":\"(NhFolYBP)in001\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}")
    );
  }

  @DisplayName("should fill 001 with HRID and drop 003/035 HRID artifacts")
  @ParameterizedTest(name = "parsedContent={0}")
  @MethodSource("fillHrIdFieldArguments")
  void shouldFillHrIdFieldInMarcRecord(String parsedContent) {
    // given
    String expectedParsedContent = "{\"leader\":\"00086nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedContent);

    Record marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    JsonObject jsonObject = new JsonObject("{\"hrid\":\"in001\"}");
    Pair<Record, JsonObject> pair = Pair.of(marcRecord, jsonObject);
    // when
    AdditionalFieldsUtil.fillHrIdFieldInMarcRecord(pair);
    // then
    Assertions.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldReturnSubfieldIfOclcExist() {
    // given
    String parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(ybp7406411)in001\"}," +
      "{\"a\":\"(OCoLC)64758\"} ],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    var expectedSubfields =  List.of("(OCoLC)64758");

    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    var subfields = get035SubfieldOclcValues(marcRecord, TAG_035).stream().map(Subfield::getData).toList();
    // then
    Assertions.assertEquals(expectedSubfields.size(), subfields.size());
    Assertions.assertEquals(expectedSubfields.getFirst(), subfields.getFirst());
  }

  @Test
  void shouldRemovePeriodsAndSpacesAfterNormalization() {
    // given
    var parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)on. 607TST .001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    var expectedParsedContent = "{\"leader\":\"00098nam  22000611a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)607TST001\"}],\"ind1\":\" \",\"ind2\":\" \"}}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(marcRecord);
    Assertions.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldPreserveOrderOf035FieldsAfterNormalization() {
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

    Record marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));
    // when
    AdditionalFieldsUtil.normalize035(marcRecord);
    Assertions.assertEquals(expectedParsedContent, parsedRecord.getContent());
  }

  @Test
  void shouldNotReturnSubfieldIfOclcNotExist() {
    // given
    String parsedContent = "{\"leader\":\"00120nam  22000731a 4500\",\"fields\":[{\"001\":\"in001\"}," +
      "{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    ParsedRecord parsedRecord = new ParsedRecord().withContent(parsedContent);

    Record marcRecord = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(parsedRecord)
      .withGeneration(0)
      .withState(Record.State.ACTUAL)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId("001").withInstanceHrid("in001"));

    // when
    var subfields = get035SubfieldOclcValues(marcRecord, TAG_035).stream().map(Subfield::getData).toList();
    // then
    Assertions.assertEquals(0, subfields.size());
  }

  @Test
  @SuppressWarnings("java:S5961")
  void caching() {
    // given
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_MARC_RECORD_PATH);
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record marcRecord = new Record().withId(UUID.randomUUID().toString()).withParsedRecord(parsedRecord);
    String instanceId = UUID.randomUUID().toString();

    CacheStats initialCacheStats = getCacheStats();

    // record with null parsed content
    Assertions.assertFalse(
      isFieldExist(new Record().withId(UUID.randomUUID().toString()), "035", 'a', instanceId));
    CacheStats cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(0, cacheStats.hitCount());
    Assertions.assertEquals(0, cacheStats.missCount());
    Assertions.assertEquals(0, cacheStats.loadCount());
    // record with empty parsed content
    Assertions.assertFalse(
      isFieldExist(
        new Record()
          .withId(UUID.randomUUID().toString())
          .withParsedRecord(new ParsedRecord().withContent("")),
        "035",
        'a',
        instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(0, cacheStats.requestCount());
    Assertions.assertEquals(0, cacheStats.hitCount());
    Assertions.assertEquals(0, cacheStats.missCount());
    Assertions.assertEquals(0, cacheStats.loadCount());
    // record with bad parsed content
    Assertions.assertFalse(
      isFieldExist(
        new Record()
          .withId(UUID.randomUUID().toString())
          .withParsedRecord(new ParsedRecord().withContent("test")),
        "035",
        'a',
        instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(1, cacheStats.requestCount());
    Assertions.assertEquals(0, cacheStats.hitCount());
    Assertions.assertEquals(1, cacheStats.missCount());
    Assertions.assertEquals(1, cacheStats.loadCount());
    // does field exists?
    Assertions.assertFalse(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(2, cacheStats.requestCount());
    Assertions.assertEquals(0, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // update field
    addDataFieldToMarcRecord(marcRecord, "035", ' ', ' ', 'a', instanceId);
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(3, cacheStats.requestCount());
    Assertions.assertEquals(1, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // verify that field exists
    Assertions.assertTrue(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(4, cacheStats.requestCount());
    Assertions.assertEquals(2, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // verify that field exists again
    Assertions.assertTrue(isFieldExist(marcRecord, "035", 'a', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(5, cacheStats.requestCount());
    Assertions.assertEquals(3, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // remove the field
    Assertions.assertTrue(removeField(marcRecord, "035"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(6, cacheStats.requestCount());
    Assertions.assertEquals(4, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // get value from controlled field
    Assertions.assertEquals("ybp7406411", getValueFromControlledField(marcRecord, "001"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(7, cacheStats.requestCount());
    Assertions.assertEquals(5, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // add controlled field to marc record
    Assertions.assertTrue(addControlledFieldToMarcRecord(marcRecord, "002", "test"));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(8, cacheStats.requestCount());
    Assertions.assertEquals(6, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
    // add field to marc record
    Assertions.assertTrue(addFieldToMarcRecord(marcRecord, AdditionalFieldsUtil.TAG_999, 'i', instanceId));
    cacheStats = getCacheStats().minus(initialCacheStats);
    Assertions.assertEquals(9, cacheStats.requestCount());
    Assertions.assertEquals(7, cacheStats.hitCount());
    Assertions.assertEquals(2, cacheStats.missCount());
    Assertions.assertEquals(2, cacheStats.loadCount());
  }

  @Test
  void isFieldsFillingNeededTrue() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder()
        .withInstanceId(instanceId)
        .withInstanceHrid(UUID.randomUUID().toString()))
      .withRecordType(Record.RecordType.MARC_BIB);

    JsonObject instanceJson = new JsonObject();
    instanceJson.put("id", instanceId);
    instanceJson.put("hrid", instanceHrId);

    Assertions.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));

    srcRecord.getExternalIdsHolder().setInstanceHrid(null);
    Assertions.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));
  }

  @Test
  void isFieldsFillingNeededFalse() {
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

  @Test
  void isFieldsFillingNeededForExternalHolderInstanceShouldThrowException() {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder()
      .withInstanceId(instanceId)
      .withInstanceHrid(instanceHrId))
      .withRecordType(Record.RecordType.MARC_BIB);

    JsonObject instanceJson = new JsonObject();
    instanceJson.put("hrid", instanceHrId);
    Assertions.assertThrows(Exception.class, () -> AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, instanceJson));
  }

  @Test
  void isFieldsFillingNeededForHoldingsExternalHolder() {
    String holdingId = UUID.randomUUID().toString();
    String holdingHrid = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withHoldingsId(holdingId))
      .withRecordType(Record.RecordType.MARC_HOLDING);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put("id", holdingId);
    jsonObject.put("hrid", holdingHrid);

    Assertions.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));

    srcRecord.getExternalIdsHolder().setHoldingsHrid(holdingHrid);

    Assertions.assertFalse(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));
  }

  @Test
  void isFieldsFillingNeededTrueForMarcAuthority() {
    String authorityId = UUID.randomUUID().toString();
    Record srcRecord = new Record().withExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(authorityId))
      .withRecordType(Record.RecordType.MARC_AUTHORITY);

    JsonObject jsonObject = new JsonObject();
    jsonObject.put("id", authorityId);

    Assertions.assertTrue(AdditionalFieldsUtil.isFieldsFillingNeeded(srcRecord, jsonObject));
  }
}
