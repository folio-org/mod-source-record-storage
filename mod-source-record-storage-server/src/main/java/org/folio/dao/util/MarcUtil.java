package org.folio.dao.util;

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

public class MarcUtil {

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  public static String rawMarcToMarcJson(String rawMarc) throws IOException, MarcException {
    Record record = rawMarcToRecord(rawMarc);
    return recordToMarcJson(record);
  }

  public static String rawMarcToTxtMarc(String rawMarc) throws IOException, MarcException {
    Record record = rawMarcToRecord(rawMarc);
    return recordToTxtMarc(record);
  }

  public static String marcJsonToRawMarc(String marcJson) throws IOException, MarcException {
    Record record = marcJsonToRecord(marcJson);
    return recordToRawMarc(record);
  }

  public static String marcJsonToTxtMarc(String marcJson) throws IOException, MarcException {
    Record record = marcJsonToRecord(marcJson);
    return recordToTxtMarc(record);
  }

  private static Record rawMarcToRecord(String rawMarc) throws IOException, MarcException {
    try (InputStream in = new ByteArrayInputStream(rawMarc.getBytes(DEFAULT_CHARSET))) {
      final MarcStreamReader reader = new MarcStreamReader(in, DEFAULT_CHARSET.name());
      if (reader.hasNext()) {
        return reader.next();
      }
    }
    throw new MarcException(String.format("Unable to read: %s", rawMarc));
  }

  private static Record marcJsonToRecord(String marcJson) throws IOException, MarcException {
    try (InputStream in = new ByteArrayInputStream(marcJson.getBytes())) {
      final MarcJsonReader reader = new MarcJsonReader(in);
      if (reader.hasNext()) {
        return reader.next();
      }
    }
    throw new MarcException(String.format("Unable to read: %s", marcJson));
  }

  private static String recordToMarcJson(Record record) throws IOException, MarcException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final MarcJsonWriter writer = new MarcJsonWriter(out);
      writer.write(record);
      writer.close();
      return out.toString();
    }
  }

  private static String recordToRawMarc(Record record) throws IOException, MarcException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final MarcStreamWriter writer = new MarcStreamWriter(out);
      writer.write(record);
      writer.close();
      return out.toString();
    }
  }

  private static String recordToTxtMarc(Record record) throws IOException, MarcException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final MarcTxtWriter writer = new MarcTxtWriter(out);
      writer.write(record);
      writer.close();
      return out.toString();
    }
  }

}