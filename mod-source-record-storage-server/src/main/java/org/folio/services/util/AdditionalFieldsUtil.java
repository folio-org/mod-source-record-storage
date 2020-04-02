package org.folio.services.util;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.rest.jaxrs.model.Record;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcWriter;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.VariableField;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  public static final String TAG_999 = "999";

  private static final String HR_ID_FROM_FIELD = "001";
  private static final String HR_ID_TO_FIELD = "035";
  private static final String HR_ID_FIELD = "hrid";

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
   * @param record - marc record
   * @param tag    - tag to read
   * @return - value from field
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

  /**
   * Move original marc hrId to 035 tag and assign created by inventory hrId into 001 tag
   *
   * @param recordInstancePair - pair of related instance and record
   */
  public static void fillHrIdFieldInMarcRecord(Pair<Record, JsonObject> recordInstancePair) {
    String hrId = recordInstancePair.getValue().getString(HR_ID_FIELD);
    String originalHrId = getValueFromControlledField(recordInstancePair.getKey(), HR_ID_FROM_FIELD);
    if (StringUtils.isNotEmpty(hrId) && StringUtils.isNotEmpty(originalHrId)) {
      removeField(recordInstancePair.getKey(), HR_ID_FROM_FIELD);
      addControlledFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_FROM_FIELD, hrId);
      addControlledFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_TO_FIELD, originalHrId);
    }
  }

  private static MarcReader buildMarcReader(Record record) {
    return new MarcJsonReader(new ByteArrayInputStream(record.getParsedRecord().getContent().toString().getBytes(StandardCharsets.UTF_8)));
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
