package org.folio.services.util;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.processing.exceptions.ReaderException;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.processing.value.Value;
import org.folio.rest.jaxrs.model.Field;
import org.folio.rest.jaxrs.model.MatchExpression;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.processing.matching.reader.util.MatchExpressionUtil.extractComparisonPart;
import static org.folio.processing.matching.reader.util.MatchExpressionUtil.isQualified;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  public static final String TAG_999 = "999";

  private static final String HR_ID_FROM_FIELD = "001";
  private static final String HR_ID_PREFIX_FROM_FIELD = "003";
  private static final String HR_ID_TO_FIELD = "035";
  private static final String HR_ID_FIELD = "hrid";
  private static final char HR_ID_FIELD_SUB = 'a';
  private static final char HR_ID_FIELD_IND = ' ';

  private static final String MARC_FIELDS_POINTER = "/fields";
  private static final String MARC_SUBFIELDS_POINTER = "/subfields";
  private static final String MARC_IND_1_FIELD_NAME = "ind1";
  private static final String MARC_IND_2_FIELD_NAME = "ind2";
  private static final String FIELD_PROFILE_LABEL = "field";
  private static final String IND_1_PROFILE_LABEL = "indicator1";
  private static final String IND_2_PROFILE_LABEL = "indicator2";
  private static final String SUBFIELD_PROFILE_LABEL = "recordSubfield";
  private static final String ASTERISK_INDICATOR = "*";

  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalFieldsUtil.class);
  private static final char INDICATOR = 'f';

  private AdditionalFieldsUtil() {
  }

  /**
   * Adds field if it does not exist and a subfield with a value to that field
   *
   * @param record   record that needs to be updated
   * @param field    field that should contain new subfield
   * @param subfield new subfield to add
   * @param value    value of the subfield to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addFieldToMarcRecord(Record record, String field, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(field), INDICATOR, INDICATOR);
          DataField dataField;
          if (variableField != null
            && ((DataField) variableField).getIndicator1() == INDICATOR
            && ((DataField) variableField).getIndicator2() == INDICATOR
          ) {
            dataField = (DataField) variableField;
            marcRecord.removeVariableField(variableField);
            dataField.removeSubfield(dataField.getSubfield(subfield));
          } else {
            dataField = factory.newDataField(field, INDICATOR, INDICATOR);
          }
          dataField.addSubfield(factory.newSubfield(subfield, value));
          marcRecord.addVariableField(dataField);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(os.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add additional subfield {} for field {} to record {}", e, subfield, field, record.getId());
    }
    return result;
  }

  /**
   * Adds new controlled field to marc record
   *
   * @param record record that needs to be updated
   * @param field  tag of controlled field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addControlledFieldToMarcRecord(Record record, String field, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          ControlField dataField = factory.newControlField(field, value);
          marcRecord.addVariableField(dataField);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(os.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add additional controlled field {) to record {}", e, field, record.getId());
    }
    return result;
  }

  /**
   * remove field from marc record
   *
   * @param record record that needs to be updated
   * @param field  tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record record, String field) {
    boolean result = false;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          VariableField variableField = marcRecord.getVariableField(field);
          if (variableField != null) {
            marcRecord.removeVariableField(variableField);
          }
          // use stream writer to recalculate leader
          marcStreamWriter.write(marcRecord);
          marcJsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(baos.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to remove controlled field {) from record {}", e, field, record.getId());
    }
    return result;
  }

  /**
   * Read value from controlled field in marc record
   *
   * @param record marc record
   * @param tag    tag to read
   * @return value from field
   */
  public static String getValueFromControlledField(Record record, String tag) {
    try {
      MarcReader reader = buildMarcReader(record);
      if (reader.hasNext()) {
        org.marc4j.marc.Record marcRecord = reader.next();
        Optional<ControlField> controlField = marcRecord.getControlFields()
          .stream()
          .filter(field -> field.getTag().equals(tag))
          .findFirst();
        if (controlField.isPresent()) {
          return controlField.get().getData();
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to read controlled field {) from record {}", e, tag, record.getId());
      return null;
    }
    return null;
  }


  public static String getValueFromField(Record record, String field, char indicator1,
                                         char indicator2, char subfield) {
    String result = null;
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(field), indicator1, indicator2);
          DataField dataField;
          if (variableField != null
            && ((DataField) variableField).getIndicator1() == INDICATOR
            && ((DataField) variableField).getIndicator2() == INDICATOR) {
            dataField = (DataField) variableField;
            result = dataField.getSubfield(subfield).getData();
          }
        }
      }
    return result;
  }


  public static Value readValueFromRecord(String marcRecord, MatchExpression matchExpression) {
    if (org.apache.commons.lang3.StringUtils.isBlank(marcRecord)) {
      return MissingValue.getInstance();
    }

    Map<String, String> matchExpressionFields = getMatchExpressionFields(matchExpression.getFields());
    List<String> marcFieldValues = readMarcFieldValues(marcRecord, matchExpressionFields)
      .stream()
      .map(marcField -> readValues(marcField, matchExpressionFields))
      .flatMap(List::stream)
      .filter(value -> isQualified(value, matchExpression.getQualifier()))
      .map(value -> extractComparisonPart(value, matchExpression.getQualifier()))
      .collect(Collectors.toList());

    if (marcFieldValues.isEmpty()) {
      return MissingValue.getInstance();
    } else if (marcFieldValues.size() == 1) {
      return StringValue.of(marcFieldValues.get(0));
    } else {
      return ListValue.of(marcFieldValues);
    }
  }

  private static Map<String, String> getMatchExpressionFields(List<Field> fields) {
    Map<String, String> resultMap = new HashMap<>();
    fields.forEach(field -> resultMap.put(field.getLabel(), field.getValue()));
    return resultMap;
  }

  private static List<JsonNode> readMarcFieldValues(String marcRecord, Map<String, String> matchExpressionFields) {
    try {
      org.folio.Record record = new ObjectMapper().readValue(marcRecord, org.folio.Record.class);
      String parsedContent = record.getParsedRecord().getContent().toString();
      JsonNode fieldsNode = new ObjectMapper().readTree(parsedContent).at(MARC_FIELDS_POINTER);
      List<JsonNode> fields = fieldsNode.findValues(matchExpressionFields.get(FIELD_PROFILE_LABEL));
      return fields.stream()
        .filter(field -> field.isTextual() || isMatchingIdentifiers(field, matchExpressionFields))
        .collect(Collectors.toList());
    } catch (IOException e) {
      throw new ReaderException("Error reading MARC record", e);
    }
  }

  private static List<String> readValues(JsonNode fieldValue, Map<String, String> matchExpressionFields) {
    if (fieldValue.isTextual()) {
      return Collections.singletonList(fieldValue.textValue());
    }
    JsonNode subfields = fieldValue.at(MARC_SUBFIELDS_POINTER);
    return subfields.findValues(matchExpressionFields.get(SUBFIELD_PROFILE_LABEL)).stream()
      .filter(JsonNode::isTextual)
      .map(JsonNode::textValue)
      .collect(Collectors.toList());
  }

  private static boolean isMatchingIdentifiers(JsonNode field, Map<String, String> matchExpressionFields) {
    boolean isFirstIndicatorMatched = isIndicatorMatched(field, matchExpressionFields, IND_1_PROFILE_LABEL, MARC_IND_1_FIELD_NAME);
    boolean isSecondIndicatorMatched = isIndicatorMatched(field, matchExpressionFields, IND_2_PROFILE_LABEL, MARC_IND_2_FIELD_NAME);
    return isFirstIndicatorMatched && isSecondIndicatorMatched;
  }

  private static boolean isIndicatorMatched(JsonNode field, Map<String, String> matchExpressionFields,
                                     String indicatorProfile, String indicatorFieldName) {
    boolean isIndicatorMatched = false;
    if (matchExpressionFields.get(indicatorProfile).equals(ASTERISK_INDICATOR)) {
      isIndicatorMatched = true;
    }
    if (matchExpressionFields.get(indicatorProfile).trim().equals(org.apache.commons.lang3.StringUtils.EMPTY)) {
      isIndicatorMatched = field.findValue(indicatorFieldName).textValue().equals(org.apache.commons.lang3.StringUtils.SPACE);
    }
    if (field.findValue(indicatorFieldName).textValue().equals(matchExpressionFields.get(indicatorProfile))) {
      isIndicatorMatched = true;
    }
    return isIndicatorMatched;
  }

  /**
   * Adds new data field to marc record
   *
   * @param record record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if succeeded, false otherwise
   */
  public static boolean addDataFieldToMarcRecord(Record record, String tag, char ind1, char ind2, char subfield, String value) {
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcReader reader = buildMarcReader(record);
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          DataField dataField = factory.newDataField(tag, ind1, ind2);
          dataField.addSubfield(factory.newSubfield(subfield, value));
          addDataFieldInNumericalOrder(dataField, marcRecord);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(new JsonObject(new String(os.toByteArray())).encode()));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to add additional data field {) to record {}", e, tag, record.getId());
    }
    return result;
  }

  private static void addDataFieldInNumericalOrder(DataField field, org.marc4j.marc.Record marcRecord) {
    String tag = field.getTag();
    List<DataField> dataFields = marcRecord.getDataFields();
    for (int i = 0; i < dataFields.size(); i++) {
      if (dataFields.get(i).getTag().compareTo(tag) > 0) {
        marcRecord.getDataFields().add(i, field);
        return;
      }
    }
    marcRecord.addVariableField(field);
  }

  /**
   * Check if data field with the same value exist
   *
   * @param record record that needs to be updated
   * @param tag    tag of data field
   * @param value  value of the field to add
   * @return true if exist
   */
  public static boolean isFieldExist(Record record, String tag, char subfield, String value) {
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcReader reader = buildMarcReader(record);
      try {
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          for (VariableField field : marcRecord.getVariableFields(tag)) {
            if (field instanceof DataField) {
              for (Subfield sub : ((DataField) field).getSubfields(subfield)) {
                if (isNotEmpty(sub.getData()) && sub.getData().equals(value.trim())) {
                  return true;
                }
              }
            } else if (field instanceof ControlField
              && isNotEmpty(((ControlField) field).getData())
              && ((ControlField) field).getData().equals(value.trim())) {
              return true;
            }
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error during the search a field in the record", e);
        return false;
      }
    }
    return false;
  }

  /**
   * Move original marc hrId to 035 tag and assign created by inventory hrId into 001 tag
   *
   * @param recordInstancePair pair of related instance and record
   */
  public static void fillHrIdFieldInMarcRecord(Pair<Record, JsonObject> recordInstancePair) {
    String hrId = recordInstancePair.getValue().getString(HR_ID_FIELD);
    String originalHrId = getValueFromControlledField(recordInstancePair.getKey(), HR_ID_FROM_FIELD);
    String originalHrIdPrefix = getValueFromControlledField(recordInstancePair.getKey(), HR_ID_PREFIX_FROM_FIELD);
    originalHrId = mergeFieldsFor035(originalHrIdPrefix, originalHrId);
    if (StringUtils.isNotEmpty(hrId) && StringUtils.isNotEmpty(originalHrId)) {
      removeField(recordInstancePair.getKey(), HR_ID_FROM_FIELD);
      removeField(recordInstancePair.getKey(), HR_ID_PREFIX_FROM_FIELD);
      addControlledFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_FROM_FIELD, hrId);
      if (!isFieldExist(recordInstancePair.getKey(), HR_ID_TO_FIELD, HR_ID_FIELD_SUB, originalHrId)) {
        addDataFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_TO_FIELD, HR_ID_FIELD_IND, HR_ID_FIELD_IND, HR_ID_FIELD_SUB, originalHrId);
      }
    }
  }

  private static String mergeFieldsFor035(String valueFrom003, String valueFrom001) {
    if (isBlank(valueFrom003)) {
      return valueFrom001;
    }
    return "(" + valueFrom003 + ")" + valueFrom001;
  }

  private static MarcReader buildMarcReader(Record record) {
    String content = ParsedRecordDaoUtil.normalizeContent(record.getParsedRecord());
    return new MarcJsonReader(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
  }

  private static VariableField getSingleFieldByIndicators(List<VariableField> list, char ind1, char ind2) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    return list.stream()
      .filter(f -> ((DataField) f).getIndicator1() == ind1 && ((DataField) f).getIndicator2() == ind2)
      .findFirst()
      .orElse(null);
  }
}
