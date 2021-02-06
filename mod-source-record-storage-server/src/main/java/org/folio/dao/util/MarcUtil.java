package org.folio.dao.util;

import static java.lang.String.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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

}
