package org.folio.dao.util;

import static java.lang.String.format;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.marc4j.MarcException;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcStreamWriter;
import org.marc4j.MarcTxtWriter;
import org.marc4j.marc.Record;

/**
 * Utility class for converting MARC records
 */
public class MarcUtil {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  private static final String MARC_RECORD_ERROR_MESSAGE = "Unable to read marc record!";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger LOGGER = LogManager.getLogger();
  public static final String FIELDS = "fields";

  private MarcUtil() { }

  /**
   * Converts raw MARC to MARC json
   *
   * @param rawMarc raw MARC
   * @return MARC json
   * @throws IOException - throws while converting MARC raw to MARC json
   */
  public static String rawMarcToMarcJson(String rawMarc) throws IOException {
    Record record = rawMarcToRecord(rawMarc);
    return recordToMarcJson(record);
  }

  /**
   * Converts raw MARC to text formatted MARC
   *
   * @param rawMarc raw MARC
   * @return text formatted MARC
   * @throws IOException - throws while converting MARC raw to formatted MARC json
   */
  public static String rawMarcToTxtMarc(String rawMarc) throws IOException {
    Record record = rawMarcToRecord(rawMarc);
    return recordToTxtMarc(record);
  }

  /**
   * Converts MARC json to raw MARC
   *
   * @param marcJson MARC json
   * @return raw MARC
   * @throws IOException - throws while converting MARC json to MARC raw
   */
  public static String marcJsonToRawMarc(String marcJson) throws IOException {
    Record record = marcJsonToRecord(marcJson);
    return recordToRawMarc(record);
  }

  /**
   * Converts MARC json to text formatted MARC
   *
   * @param marcJson MARC json
   * @return text formatted MARC
   * @throws IOException - throws while converting MARC json to MARC text formatted
   */
  public static String marcJsonToTxtMarc(String marcJson) throws IOException {
    Record record = marcJsonToRecord(marcJson);
    return recordToTxtMarc(record);
  }

  private static Record rawMarcToRecord(String rawMarc) throws IOException {
    try (InputStream in = new ByteArrayInputStream(rawMarc.getBytes(DEFAULT_CHARSET))) {
      final MarcStreamReader reader = new MarcStreamReader(in, DEFAULT_CHARSET.name());
      if (reader.hasNext()) {
        return reader.next();
      }
    }
    throw new MarcException(format("Unable to read: %s", rawMarc));
  }

  private static Record marcJsonToRecord(String marcJson) throws IOException {
    try (InputStream in = new ByteArrayInputStream(marcJson.getBytes())) {
      final MarcJsonReader reader = new MarcJsonReader(in);
      if (reader.hasNext()) {
        try {
          return reader.next();
        } catch (Exception e) {
          throw new MarcException(MARC_RECORD_ERROR_MESSAGE);
        }
      }
    }
    throw new MarcException(format("Unable to read: %s", marcJson));
  }

  private static String recordToMarcJson(Record record) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final MarcJsonWriter writer = new MarcJsonWriter(out);
      writer.write(record);
      writer.close();
      return out.toString();
    }
  }

  private static String recordToRawMarc(Record record) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final MarcStreamWriter writer = new MarcStreamWriter(out);
      writer.write(record);
      writer.close();
      return out.toString();
    }
  }

  private static String recordToTxtMarc(Record record) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final MarcTxtWriter writer = new MarcTxtWriter(out);
      writer.write(record);
      writer.close();
      return out.toString();
    }
  }

  /**
   * Reorders MARC record fields
   *
   * @param sourceContent source parsed record
   * @param targetContent target parsed record
   * @return MARC txt
   */
  public static String reorderMarcRecordFields(String sourceContent, String targetContent) {
    try {
      var parsedContent = objectMapper.readTree(targetContent);
      var fieldsArrayNode = (ArrayNode) parsedContent.path(FIELDS);

      var jsonNodesByTag = groupNodesByTag(fieldsArrayNode);
      var sourceFields = getSourceFields(sourceContent);
      var rearrangedArray = objectMapper.createArrayNode();

      var nodes001 = jsonNodesByTag.get(HR_ID_FROM_FIELD);
      if (nodes001 != null && !nodes001.isEmpty()) {
        rearrangedArray.addAll(nodes001);
        jsonNodesByTag.remove(HR_ID_FROM_FIELD);
      }

      var nodes005 = jsonNodesByTag.get(TAG_005);
      if (nodes005 != null && !nodes005.isEmpty()) {
        rearrangedArray.addAll(nodes005);
        jsonNodesByTag.remove(TAG_005);
      }

      for (String tag : sourceFields) {
          Queue<JsonNode> nodes = jsonNodesByTag.get(tag);
          if (nodes != null && !nodes.isEmpty()) {
            rearrangedArray.addAll(nodes);
            jsonNodesByTag.remove(tag);
          }

      }

      jsonNodesByTag.values().forEach(rearrangedArray::addAll);

      ((ObjectNode) parsedContent).set(FIELDS, rearrangedArray);
      return parsedContent.toString();
    } catch (Exception e) {
      LOGGER.error("An error occurred while reordering Marc record fields: {}", e.getMessage(), e);
      return targetContent;
    }
  }

  private static Map<String, Queue<JsonNode>> groupNodesByTag(ArrayNode fieldsArrayNode) {
    var jsonNodesByTag = new LinkedHashMap<String, Queue<JsonNode>>();
    for (JsonNode node : fieldsArrayNode) {
      var tag = getTagFromNode(node);
      jsonNodesByTag.putIfAbsent(tag, new LinkedList<>());
      jsonNodesByTag.get(tag).add(node);
    }
    return jsonNodesByTag;
  }

  private static String getTagFromNode(JsonNode node) {
    return node.fieldNames().next();
  }

  private static List<String> getSourceFields(String source) {
    var sourceFields = new ArrayList<String>();
    var remainingFields = new ArrayList<String>();
    var has001 = false;
    try {
      var sourceJson = objectMapper.readTree(source);
      var fieldsNode = sourceJson.get(FIELDS);

      for (JsonNode fieldNode : fieldsNode) {
        var tag = fieldNode.fieldNames().next();
        if (tag.equals(HR_ID_FROM_FIELD)) {
          sourceFields.add(0, tag);
          has001 = true;
        } else if (tag.equals(TAG_005)) {
          if (!has001) {
            sourceFields.add(0, tag);
          } else {
            sourceFields.add(1, tag);
          }
        } else {
          remainingFields.add(tag);
        }
      }
      sourceFields.addAll(remainingFields);
    } catch (Exception e) {
      LOGGER.error("An error occurred while parsing source JSON: {}", e.getMessage(), e);
    }
    return sourceFields;
  }
}
