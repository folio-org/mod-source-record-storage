package org.folio.services.util;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.dao.util.MarcUtil.reorderMarcRecordFields;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.ParsedRecordDaoUtil;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MarcFieldProtectionSetting;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.exceptions.PostProcessingException;
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


/**
 * Util to work with additional fields
 */
public final class AdditionalFieldsUtil {

  public static final String TAG_00X_PREFIX = "00";
  public static final String TAG_005 = "005";
  public static final String TAG_999 = "999";
  public static final String TAG_035 = "035";

  public static final String HR_ID_FROM_FIELD = "001";
  private static final String HR_ID_PREFIX_FROM_FIELD = "003";
  private static final String HR_ID_TO_FIELD = "035";
  private static final String HR_ID_FIELD = "hrid";
  private static final String ID_FIELD = "id";
  private static final char HR_ID_FIELD_SUB = 'a';
  private static final char HR_ID_FIELD_IND = ' ';
  private static final String ANY_STRING = "*";
  private static final String OCLC = "OCoLC";
  private static final String OCLC_PREFIX = "(OCoLC)";
  private static final String OCLC_PATTERN = "\\((" + OCLC + ")\\)((ocm|ocn|on)?0*|([a-zA-Z]+)0*)(\\d+\\w*)";


  public static final DateTimeFormatter dateTime005Formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss.S");

  private static final Logger LOGGER = LogManager.getLogger();
  private static final char INDICATOR = 'f';

  private static final CacheLoader<String, org.marc4j.marc.Record> parsedRecordContentCacheLoader;
  private static final LoadingCache<String, org.marc4j.marc.Record> parsedRecordContentCache;

  static {
    // this function is executed when creating a new item to be saved in the cache.
    // In this case, this is a MARC4J Record
    parsedRecordContentCacheLoader =
      parsedRecordContent -> {
        MarcJsonReader marcJsonReader =
          new MarcJsonReader(
            new ByteArrayInputStream(
              parsedRecordContent.getBytes(StandardCharsets.UTF_8)));
        if (marcJsonReader.hasNext()) {
          return marcJsonReader.next();
        }
        return null;
      };

    parsedRecordContentCache =
      Caffeine.newBuilder()
        .maximumSize(2000)
        // weak keys allows parsed content strings that are used as keys to be garbage
        // collected, even it is still
        // referenced by the cache.
        .weakKeys()
        .recordStats()
        .executor(
          serviceExecutor -> {
            // Due to the static nature and the API of this AdditionalFieldsUtil class, it is difficult to
            // pass a vertx instance or assume whether a call to any of its static methods here is by a Vertx
            // thread or a regular thread. The logic before is able to discern the type of thread and execute
            // cache operations using the appropriate threading model.
            Context context = Vertx.currentContext();
            if (context != null) {
              context.runOnContext(ar -> serviceExecutor.run());
            } else {
              // The common pool below is used because it is the  default executor for caffeine
              ForkJoinPool.commonPool().execute(serviceExecutor);
            }
          })
        .build(parsedRecordContentCacheLoader);
  }

  private AdditionalFieldsUtil() {
  }

  /**
   * Get cache stats
   */
  public static CacheStats getCacheStats() {
    return parsedRecordContentCache.stats();
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
        var sourceParsedRecordString = record.getParsedRecord().getContent().toString();
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
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

          String parsedContentString = new JsonObject(os.toString()).encode();
          var content = reorderMarcRecordFields(sourceParsedRecordString, parsedContentString);
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(content, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(content));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addFieldToMarcRecord:: Failed to add additional subfield {} for field {} to record {}",
        subfield, field, record != null ? record.getId() : null, e);
    }
    return result;
  }

  public static String getFieldFromMarcRecord(Record record, String field, char ind1, char ind2, char subfield) {
    org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
    if (marcRecord != null) {
      List<VariableField> variableFields = marcRecord.getVariableFields(field);
      Optional<DataField> dataField = variableFields.stream().filter(v -> v instanceof DataField)
        .map(v -> (DataField) v)
        .filter(v -> ind1 == v.getIndicator1() && ind2 == v.getIndicator2())
        .findFirst();
      return dataField.map(value -> value.getSubfieldsAsString(String.valueOf(subfield))).orElse(null);
    }
    return null;
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
    return addControlledFieldToMarcRecord(record, field, value, false);
  }

  public static boolean addControlledFieldToMarcRecord(Record record, String field, String value, boolean replace) {
    LOGGER.debug("addControlledFieldToMarcRecord:: Started adding controlled field '{}' with value '{}' to record '{}'", field, value, record.getId());
    boolean result = false;
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        LOGGER.debug("addControlledFieldToMarcRecord:: Check conditions before adding controlled field '{}' with value '{}' to record '{}'", field, value, record.getId());
        if (replace) {
          LOGGER.debug("addControlledFieldToMarcRecord:: Removing controlled field '{}' from record '{}' before changes", field, record.getId());
          removeField(record, field);
        }
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);

        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          LOGGER.debug("addControlledFieldToMarcRecord:: Adding controlled field {} with value {} to record {}", field, value, record.getId());
          ControlField dataField = factory.newControlField(field, value);
          marcRecord.addVariableField(dataField);
          // use stream writer to recalculate leader
          LOGGER.debug("addControlledFieldToMarcRecord:: Writing by streamWriter controlled field {} with value {} to record {}", field, value, record.getId());
          streamWriter.write(marcRecord);

          LOGGER.debug("addControlledFieldToMarcRecord:: Writing by jsonWriter controlled field {} with value {} to record {}", field, value, record.getId());
          jsonWriter.write(marcRecord);

          LOGGER.debug("addControlledFieldToMarcRecord:: Prepared parsedContentString for record {}", record.getId());
          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      } else {
        LOGGER.warn("addControlledFieldToMarcRecord:: Record or parsed record content is null for record {}", record.getId());
      }
    } catch (Exception e) {
      LOGGER.warn("addControlledFieldToMarcRecord:: Failed to add additional controlled field {} to record {}", field, record.getId(), e);
    }
    return result;
  }

  /**
   * remove field from marc record
   *
   * @param record    record that needs to be updated
   * @param fieldName tag of the field
   * @param subfield  subfield of the field
   * @param value     value of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record record, String fieldName, char subfield, String value) {
    boolean isFieldRemoveSucceed = false;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
        MarcWriter marcStreamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter marcJsonWriter = new MarcJsonWriter(baos);
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          LOGGER.debug("removeField:: Started removing controlled field {} with value {} from record {}", fieldName, value, record.getId());
          if (StringUtils.isEmpty(value)) {
            isFieldRemoveSucceed = removeFirstFoundFieldByName(marcRecord, fieldName);
          } else {
            isFieldRemoveSucceed = removeFieldByNameAndValue(marcRecord, fieldName, subfield, value);
          }

          LOGGER.debug("removeField:: Removing controlled field {} with value {} from record {} is {}", fieldName, value, record.getId(), isFieldRemoveSucceed);
          if (isFieldRemoveSucceed) {
            LOGGER.debug("removeField:: Writing record {} after removing controlled field {} with value {}", record.getId(), fieldName, value);
            // use stream writer to recalculate leader
            marcStreamWriter.write(marcRecord);

            LOGGER.debug("removeField:: Writing record {} after removing controlled field {} with value {} by jsonWriter", record.getId(), fieldName, value);
            marcJsonWriter.write(marcRecord);

            String parsedContentString = new JsonObject(baos.toString()).encode();

            LOGGER.debug("removeField:: Prepared parsedContentString for record {}", record.getId());
            // save parsed content string to cache then set it on the record
            parsedRecordContentCache.put(parsedContentString, marcRecord);
            record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          }
        }
      } else {
        if (record != null) {
          LOGGER.warn("removeField:: Record or parsed record content is null for record {}", record.getId());
        } else {
          LOGGER.warn("removeField:: Record is null");
        }
      }
    } catch (Exception e) {
      if (record != null) {
        LOGGER.warn("removeField:: Failed to remove controlled field {} from record {}", fieldName, record.getId(), e);
      } else {
        LOGGER.warn("removeField:: Failed to remove controlled field {} from record", fieldName, e);
      }
    }
    return isFieldRemoveSucceed;
  }

  private static boolean removeFirstFoundFieldByName(org.marc4j.marc.Record marcRecord, String fieldName) {
    boolean isFieldFound = false;
    VariableField variableField = marcRecord.getVariableField(fieldName);
    if (variableField != null) {
      marcRecord.removeVariableField(variableField);
      isFieldFound = true;
    }
    return isFieldFound;
  }

  private static boolean removeFieldByNameAndValue(org.marc4j.marc.Record marcRecord, String fieldName, char subfield, String value) {
    boolean isFieldFound = false;
    List<VariableField> variableFields = marcRecord.getVariableFields(fieldName);
    for (VariableField variableField : variableFields) {
      if (isFieldContainsValue(variableField, subfield, value)) {
        marcRecord.removeVariableField(variableField);
        isFieldFound = true;
        break;
      }
    }
    return isFieldFound;
  }

  /**
   * Checks if the field contains a certain value in the selected subfield
   *
   * @param field    from MARC BIB record
   * @param subfield subfield of the field
   * @param value    value of the field
   * @return true if contains, false otherwise
   */
  private static boolean isFieldContainsValue(VariableField field, char subfield, String value) {
    boolean isContains = false;
    if (field instanceof DataField) {
      for (Subfield sub : ((DataField) field).getSubfields(subfield)) {
        if (isNotEmpty(sub.getData()) && sub.getData().contains(value.trim())) {
          isContains = true;
          break;
        }
      }
    }
    return isContains;
  }

  /**
   * remove field from marc record
   *
   * @param record record that needs to be updated
   * @param field  tag of the field
   * @return true if succeeded, false otherwise
   */
  public static boolean removeField(Record record, String field) {
    return removeField(record, field, '\0', null);
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
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
        Optional<ControlField> controlField = marcRecord.getControlFields()
          .stream()
          .filter(field -> field.getTag().equals(tag))
          .findFirst();
        if (controlField.isPresent()) {
          return controlField.get().getData();
        }
      }
    } catch (Exception e) {
      LOGGER.warn("getValueFromControlledField:: Failed to read controlled field {} from record {}", tag, record.getId(), e);
      return null;
    }
    return null;
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
        MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
        MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
        MarcFactory factory = MarcFactory.newInstance();
        org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
        if (marcRecord != null) {
          DataField dataField = factory.newDataField(tag, ind1, ind2);
          dataField.addSubfield(factory.newSubfield(subfield, value));
          addDataFieldInNumericalOrder(dataField, marcRecord);
          // use stream writer to recalculate leader
          streamWriter.write(marcRecord);
          jsonWriter.write(marcRecord);

          String parsedContentString = new JsonObject(os.toString()).encode();
          // save parsed content string to cache then set it on the record
          parsedRecordContentCache.put(parsedContentString, marcRecord);
          record.setParsedRecord(record.getParsedRecord().withContent(parsedContentString));
          result = true;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("addDataFieldToMarcRecord:: Failed to add additional data field {} to record {}",
        tag, record != null ? record.getId() : null, e);
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
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(record);
      if (marcRecord != null) {
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
      LOGGER.warn("isFieldExist:: Error during the search a field in the record", e);
      return false;
    }
    return false;
  }

  public static void normalize035(Record srsRecord) {
    List<Subfield> subfields = get035SubfieldOclcValues(srsRecord, TAG_035);
    if (!subfields.isEmpty()) {
      LOGGER.debug("normalize035:: normalizing 035 field of a record with id: {}", srsRecord.getId());
      formatOclc(subfields);
      deduplicateOclc(srsRecord, subfields, TAG_035);
      recalculateLeaderAndParsedRecord(srsRecord);
    }
  }

  private static void formatOclc(List<Subfield> subfields) {
    Pattern pattern = Pattern.compile(OCLC_PATTERN);

    for (Subfield subfield : subfields) {
      String data = subfield.getData().replaceAll("[.\\s]", "");
      Matcher matcher = pattern.matcher(data);
      if (matcher.find()) {
        String oclcTag = matcher.group(1); // "OCoLC"
        String numericAndTrailing = matcher.group(5); // Numeric part and any characters that follow
        String prefix = matcher.group(2); // Entire prefix including letters and potentially leading zeros

        if (prefix != null && (prefix.startsWith("ocm") || prefix.startsWith("ocn") || prefix.startsWith("on"))) {
          // If "ocm" or "ocn", strip entirely from the prefix
          subfield.setData("(" + oclcTag + ")" + numericAndTrailing);
        } else {
          // For other cases, strip leading zeros only from the numeric part
          numericAndTrailing = numericAndTrailing.replaceFirst("^0+", "");
          if (prefix != null) {
            prefix = prefix.replaceAll("\\d+", ""); // Safely remove digits from the prefix if not null
          }
          // Add back any other prefix that might have been included like "tfe"
          subfield.setData("(" + oclcTag + ")" + (prefix != null ? prefix : "") + numericAndTrailing);
        }
      }
    }
  }

  private static void deduplicateOclc(Record srcRecord, List<Subfield> subfields, String tag) {
    List<Subfield> subfieldsToDelete = new ArrayList<>();

    for (Subfield subfield: new ArrayList<>(subfields)) {
      if (subfields.stream().anyMatch(s -> isOclcSubfieldDuplicated(subfield, s))) {
        subfieldsToDelete.add(subfield);
        subfields.remove(subfield);
      }
    }
    Optional.ofNullable(computeMarcRecord(srcRecord)).ifPresent(marcRecord -> {
      List<VariableField> variableFields = marcRecord.getVariableFields(tag);

      subfieldsToDelete.forEach(subfieldToDelete ->
        variableFields.forEach(field -> removeSubfieldIfExist(marcRecord, field, subfieldToDelete)));
    });
  }

  private static boolean isOclcSubfieldDuplicated(Subfield s1, Subfield s2) {
    return s1 != s2 && s1.getData().equals(s2.getData()) && s1.getCode() == s2.getCode();
  }

  private static void removeSubfieldIfExist(org.marc4j.marc.Record marcRecord, VariableField field, Subfield subfieldToDelete) {
    if (field instanceof DataField dataField && dataField.getSubfields().contains(subfieldToDelete)) {
      if (dataField.getSubfields().size() > 1) {
        dataField.removeSubfield(subfieldToDelete);
      } else {
        marcRecord.removeVariableField(dataField);
      }
    }
  }

  private static void recalculateLeaderAndParsedRecord(Record recordForUpdate) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      MarcWriter streamWriter = new MarcStreamWriter(new ByteArrayOutputStream());
      MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
      org.marc4j.marc.Record marcRecord = computeMarcRecord(recordForUpdate);

      if (marcRecord != null) {
        // use stream writer to recalculate leader
        streamWriter.write(marcRecord);
        jsonWriter.write(marcRecord);

        String parsedContentString = new JsonObject(os.toString()).encode();
        // save parsed content string to cache then set it on the record
        parsedRecordContentCache.put(parsedContentString, marcRecord);
        recordForUpdate.setParsedRecord(recordForUpdate.getParsedRecord().withContent(parsedContentString));
      }
    } catch (Exception e) {
      LOGGER.warn("recalculateLeaderAndParsedRecord:: Failed to recalculate leader and parsed record for record: {}", recordForUpdate.getId(), e);
    }
  }

  public static List<Subfield> get035SubfieldOclcValues(Record srcRecord, String tag) {
    return Optional.ofNullable(computeMarcRecord(srcRecord))
      .stream()
      .flatMap(marcRecord -> marcRecord.getVariableFields(tag).stream())
      .flatMap(field -> get035oclcSubfields(field).stream())
      .collect(Collectors.toList());
  }

  private static List<Subfield> get035oclcSubfields(VariableField field) {
    if (field instanceof DataField dataField) {
      return dataField.getSubfields().stream()
        .filter(sf -> sf.getData().trim().startsWith(OCLC_PREFIX))
        .toList();
    }
    return Collections.emptyList();
  }

  /**
   * Move original marc hrId to 035 tag and assign created by inventory hrId into 001 tag
   *
   * @param recordInstancePair pair of related instance and record
   */
  public static void fillHrIdFieldInMarcRecord(Pair<Record, JsonObject> recordInstancePair) {
    String hrId = recordInstancePair.getValue().getString(HR_ID_FIELD);
    String valueFrom001 = getValueFromControlledField(recordInstancePair.getKey(), HR_ID_FROM_FIELD);
    if (!StringUtils.equals(hrId, valueFrom001)) {
      if (StringUtils.isNotEmpty(valueFrom001)) {
        String originalHrIdPrefix = getValueFromControlledField(recordInstancePair.getKey(), HR_ID_PREFIX_FROM_FIELD);
        String originalHrId = mergeFieldsFor035(originalHrIdPrefix, valueFrom001);
        if (!isFieldExist(recordInstancePair.getKey(), HR_ID_TO_FIELD, HR_ID_FIELD_SUB, originalHrId)) {
          addDataFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_TO_FIELD, HR_ID_FIELD_IND, HR_ID_FIELD_IND, HR_ID_FIELD_SUB, originalHrId);
        }
      }
      removeField(recordInstancePair.getKey(), HR_ID_FROM_FIELD);
      if (StringUtils.isNotEmpty(hrId)) {
        addControlledFieldToMarcRecord(recordInstancePair.getKey(), HR_ID_FROM_FIELD, hrId);
      }
    } else {
      remove035WithActualHrId(recordInstancePair.getKey(), hrId);
    }
    removeField(recordInstancePair.getKey(), HR_ID_PREFIX_FROM_FIELD);
  }

  public static void fill035FieldInMarcRecordIfNotExists(Record record, String incoming001) {
    String originalHrIdPrefix = getValueFromControlledField(record, HR_ID_PREFIX_FROM_FIELD);
    String incoming035 = mergeFieldsFor035(originalHrIdPrefix, incoming001);
    if (StringUtils.isNotEmpty(incoming001) && !isFieldExist(record, HR_ID_TO_FIELD, HR_ID_FIELD_SUB, incoming035)) {
      addDataFieldToMarcRecord(record, HR_ID_TO_FIELD, HR_ID_FIELD_IND, HR_ID_FIELD_IND, HR_ID_FIELD_SUB, incoming035);
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

  /**
   * Updates field 005 for case when this field is not protected.
   *
   * @param targetRecord            record to update
   * @param mappingParameters mapping parameters
   */
  public static void updateLatestTransactionDate(Record targetRecord, MappingParameters mappingParameters) {
    LOGGER.debug("updateLatestTransactionDate(1):: Updating field '005' for record with id '{}'", targetRecord.getId());
    try {
      if (isField005NeedToUpdate(targetRecord, mappingParameters)) {
        updateLatestTransactionDate(targetRecord);
      }
    } catch (Exception ex) {
      LOGGER.error("updateLatestTransactionDate(1):: Failed to update field '005' for record with id '{}'", targetRecord.getId(), ex);
      throw new PostProcessingException(format("Failed to update field '005' to record with id '%s'", targetRecord.getId()));
    }
  }

  /**
   * Updates field 005.
   * @param targetRecord            record to update
   */
  public static void updateLatestTransactionDate(Record targetRecord) {
    LOGGER.debug("updateLatestTransactionDate(2):: Updating field '005' for record with id '{}'", targetRecord.getId());
    String date = AdditionalFieldsUtil.dateTime005Formatter.format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
    boolean isLatestTransactionDateUpdated = AdditionalFieldsUtil.addControlledFieldToMarcRecord(targetRecord, AdditionalFieldsUtil.TAG_005, date, true);
    if (!isLatestTransactionDateUpdated) {
      LOGGER.error("updateLatestTransactionDate(2):: Failed to update field '005' to record with id '{}'", targetRecord.getId());
      throw new PostProcessingException(format("updateLatestTransactionDate(2):: Failed to update field '005' to record with id '%s'", targetRecord.getId()));
    }
  }

  /**
   * Remove 003 field if hrid is not empty (from instance and marc-record)
   *
   * @param record       - source record
   * @param instanceHrid - existing instanceHrid
   */
  public static void remove003FieldIfNeeded(Record record, String instanceHrid) {
    if (StringUtils.isNotBlank(instanceHrid) && StringUtils.isNotBlank(AdditionalFieldsUtil.getValueFromControlledField(record, "001"))) {
      AdditionalFieldsUtil.removeField(record, HR_ID_PREFIX_FROM_FIELD);
    }
  }

  /**
   * Remove 035 field if 035 equals actual HrId if exists
   *
   * @param record     - source record
   * @param actualHrId - actual HrId
   */
  public static void remove035WithActualHrId(Record record, String actualHrId) {
    removeField(record, HR_ID_TO_FIELD, HR_ID_FIELD_SUB, actualHrId);
  }

  /**
   * Check if record should be filled by specific fields.
   *
   * @param record         - source record.
   * @param externalEntity - source externalEntity.
   * @return - true if need.
   */
  public static boolean isFieldsFillingNeeded(Record record, JsonObject externalEntity) {
    var recordType = record.getRecordType();
    var externalIdsHolder = record.getExternalIdsHolder();
    var id = externalEntity.getString(ID_FIELD);
    var hrid = externalEntity.getString(HR_ID_FIELD);
    if (Record.RecordType.MARC_BIB == recordType) {
      return isValidIdAndHrid(id, hrid, externalIdsHolder.getInstanceId(), externalIdsHolder.getInstanceHrid());
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return isValidIdAndHrid(id, hrid, externalIdsHolder.getHoldingsId(), externalIdsHolder.getHoldingsHrid());
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      return isValidId(id, externalIdsHolder.getAuthorityId());
    } else {
      return false;
    }
  }

  private static boolean isValidIdAndHrid(String id, String hrid, String externalId, String externalHrid) {
    return (isNotEmpty(externalId)) && (id.equals(externalId) && !hrid.equals(externalHrid));
  }

  private static boolean isValidId(String id, String externalId) {
    return isNotEmpty(externalId) && id.equals(externalId);
  }

  /**
   * Checks whether field 005 needs to be updated or this field is protected.
   *
   * @param record            record to check
   * @param mappingParameters
   * @return true for case when field 005 have to updated
   */
  private static boolean isField005NeedToUpdate(Record record, MappingParameters mappingParameters) {
    LOGGER.debug("isField005NeedToUpdate:: Checking if field '005' needs to be updated for record with id '{}'", record.getId());
    boolean needToUpdate = true;
    try {
      List<MarcFieldProtectionSetting> fieldProtectionSettings = mappingParameters.getMarcFieldProtectionSettings();
      if ((fieldProtectionSettings != null) && !fieldProtectionSettings.isEmpty()) {
        LOGGER.debug("isField005NeedToUpdate:: Checking if field '005' is protected for record with id '{}'", record.getId());
        MarcReader reader = new MarcJsonReader(new ByteArrayInputStream(record.getParsedRecord().getContent().toString().getBytes()));
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          for (VariableField field : marcRecord.getVariableFields(AdditionalFieldsUtil.TAG_005)) {
            needToUpdate = isNotProtected(fieldProtectionSettings, (ControlField) field);
            break;
          }
        }
      }
    } catch (Exception ex) {
      LOGGER.error("isField005NeedToUpdate:: Failed to check if field '005' needs to be updated for record with id '{}'", record.getId(), ex);
      return needToUpdate;
    }
    LOGGER.debug("isField005NeedToUpdate:: Field '005' needs to be updated for record with id '{}': {}", record.getId(), needToUpdate);
    return needToUpdate;
  }

  /**
   * Checks is the control field is protected or not.
   *
   * @param fieldProtectionSettings List of MarcFieldProtectionSettings
   * @param field                   Control field that is being checked
   * @return true for case when control field isn't protected
   */
  private static boolean isNotProtected(List<MarcFieldProtectionSetting> fieldProtectionSettings, ControlField field) {
    return fieldProtectionSettings.stream()
      .filter(setting -> setting.getField().equals(ANY_STRING) || setting.getField().equals(field.getTag()))
      .noneMatch(setting -> setting.getData().equals(ANY_STRING) || setting.getData().equals(field.getData()));
  }

  private static org.marc4j.marc.Record computeMarcRecord(Record record) {
    if (record != null
      && record.getParsedRecord() != null
      && isNotBlank(record.getParsedRecord().getContent().toString())) {
      try {
        var content = normalizeContent(record.getParsedRecord().getContent());
        return parsedRecordContentCache.get(content);
      } catch (Exception e) {
        LOGGER.warn("computeMarcRecord:: Error during the transformation to marc record", e);
        try {
          MarcReader reader = buildMarcReader(record);
          if (reader.hasNext()) {
            return reader.next();
          }
        } catch (Exception ex) {
          LOGGER.warn("computeMarcRecord:: Error during the building of MarcReader", ex);
        }
        return null;
      }
    }
    return null;
  }

  private static String normalizeContent(Object content) {
    return (content instanceof String)
      ? (String) content
      : Json.encode(content);
  }

  /**
   * Check if any field with the subfield code exists.
   *
   * @param sourceRecord - source record.
   * @param subFieldCode - subfield code.
   * @return true if exists, otherwise false.
   */
  public static boolean isSubfieldExist(Record sourceRecord, char subFieldCode) {
    try {
      org.marc4j.marc.Record marcRecord = computeMarcRecord(sourceRecord);
      if (marcRecord != null) {
        for (DataField dataField : marcRecord.getDataFields()) {
          Subfield subfield = dataField.getSubfield(subFieldCode);
          if (subfield != null) {
            return true;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("isSubfieldExist:: Error during the search a subfield in the record", e);
      return false;
    }
    return false;
  }

}
