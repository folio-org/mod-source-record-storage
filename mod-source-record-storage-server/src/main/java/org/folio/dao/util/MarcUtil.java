package org.folio.dao.util;

import static java.lang.String.format;
import static org.folio.services.util.AdditionalFieldsUtil.HR_ID_FROM_FIELD;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_00X_PREFIX;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
   * Take field values from system modified record content while preserving incoming record content`s field order.
   * Put system fields (001, 005) first, regardless of incoming record fields order.
   *
   * @param sourceOrderContent content with incoming record fields order
   * @param systemOrderContent system modified record content with reordered fields
   * @return MARC record parsed content with desired fields order
   */
  public static String reorderMarcRecordFields(String sourceOrderContent, String systemOrderContent) {
    try {
      var parsedContent = objectMapper.readTree(systemOrderContent);
      var fieldsArrayNode = (ArrayNode) parsedContent.path(FIELDS);

      var nodes = toNodeList(fieldsArrayNode);
      var nodes00X = removeAndGetNodesByTagPrefix(nodes, TAG_00X_PREFIX);
      var sourceOrderTags = getSourceFields(sourceOrderContent);
      var reorderedFields = objectMapper.createArrayNode();

      var node001 = removeAndGetNodeByTag(nodes00X, HR_ID_FROM_FIELD);
      if (node001 != null && !node001.isEmpty()) {
        reorderedFields.add(node001);
      }

      var node005 = removeAndGetNodeByTag(nodes00X, TAG_005);
      if (node005 != null && !node005.isEmpty()) {
        reorderedFields.add(node005);
      }

      for (String tag : sourceOrderTags) {
        var nodeTag = tag;
        //loop will add system generated fields that are absent in initial record, preserving their order, f.e. 035
        do {
          var node = tag.startsWith(TAG_00X_PREFIX) ? removeAndGetNodeByTag(nodes00X, tag) : nodes.remove(0);
          if (node != null && !node.isEmpty()) {
            nodeTag = getTagFromNode(node);
            reorderedFields.add(node);
          }
        } while (!tag.equals(nodeTag) && !nodes.isEmpty());
      }

      reorderedFields.addAll(nodes);

      ((ObjectNode) parsedContent).set(FIELDS, reorderedFields);
      return parsedContent.toString();
    } catch (Exception e) {
      LOGGER.error("An error occurred while reordering Marc record fields: {}", e.getMessage(), e);
      return systemOrderContent;
    }
  }

  private static List<JsonNode> toNodeList(ArrayNode fieldsArrayNode) {
    var nodes = new LinkedList<JsonNode>();
    for (var node : fieldsArrayNode) {
      nodes.add(node);
    }
    return nodes;
  }

  private static JsonNode removeAndGetNodeByTag(List<JsonNode> nodes, String tag) {
    for (int i = 0; i < nodes.size(); i++) {
      var nodeTag = getTagFromNode(nodes.get(i));
      if (nodeTag.equals(tag)) {
        return nodes.remove(i);
      }
    }
    return null;
  }

  private static List<JsonNode> removeAndGetNodesByTagPrefix(List<JsonNode> nodes, String prefix) {
    var startsWithNodes = new LinkedList<JsonNode>();
    for (int i = 0; i < nodes.size(); i++) {
      var nodeTag = getTagFromNode(nodes.get(i));
      if (nodeTag.startsWith(prefix)) {
        startsWithNodes.add(nodes.get(i));
      }
    }

    nodes.removeAll(startsWithNodes);
    return startsWithNodes;
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
