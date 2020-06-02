package org.folio;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.tools.utils.ObjectMapperTool;

import io.vertx.core.json.JsonObject;

public class TestMocks {

  private static final String SOURCE_RECORDS_FOLDER_PATH = "src/test/resources/mock/sourceRecords";

  private static final String SNAPSHOT_PATH_TEMPLATE = "src/test/resources/mock/snapshots/%s.json";
  private static final String RECORD_PATH_TEMPLATE = "src/test/resources/mock/records/%s.json";
  private static final String ERROR_RECORD_PATH_TEMPLATE = "src/test/resources/mock/errorRecords/%s.json";

  private static List<Snapshot> snapshots;

  private static List<Record> records;

  private static List<RawRecord> rawRecords;

  private static List<ParsedRecord> parsedRecords;

  private static List<ErrorRecord> errorRecords;

  static { 
    List<SourceRecord> sourceRecords = readSourceRecords();
    rawRecords = sourceRecords.stream().map(TestMocks::toRawRecord).collect(Collectors.toList());
    parsedRecords = sourceRecords.stream().map(TestMocks::toParsedRecord).collect(Collectors.toList());
    errorRecords = readErrorRecords(sourceRecords);
    records = readRecords(sourceRecords);
    snapshots = readSnapshots(sourceRecords);
  }

  public static List<Snapshot> getSnapshots() {
    return new ArrayList<>(snapshots);
  }

  public static Snapshot getSnapshot(int index) {
    return snapshots.get(index);
  }

  public static List<Record> getRecords() {
    return new ArrayList<>(records);
  }

  public static Record getRecord(int index) {
    return records.get(index);
  }

  public static List<ErrorRecord> getErrorRecords() {
    return new ArrayList<>(errorRecords);
  }

  public static ErrorRecord getErrorRecord(int index) {
    return errorRecords.get(index);
  }

  public static List<RawRecord> getRawRecords() {
    return new ArrayList<>(rawRecords);
  }

  public static RawRecord getRawRecord(int index) {
    return rawRecords.get(index);
  }

  public static List<ParsedRecord> getParsedRecords() {
    return new ArrayList<>(parsedRecords);
  }

  public static ParsedRecord getParsedRecord(int index) {
    return parsedRecords.get(index);
  }

  public static Optional<Snapshot> getSnapshot(String id) {
    return snapshots.stream().filter(s -> s.getJobExecutionId().equals(id)).findAny();
  }

  public static Optional<Record> getRecord(String id) {
    return records.stream().filter(r -> r.getId().equals(id)).findAny();
  }

  public static Optional<ErrorRecord> getErrorRecord(String id) {
    return errorRecords.stream().filter(er -> er.getId().equals(id)).findAny();
  }

  public static Optional<RawRecord> getRawRecord(String id) {
    return rawRecords.stream().filter(rr -> rr.getId().equals(id)).findAny();
  }

  public static Optional<ParsedRecord> getParsedRecord(String id) {
    return parsedRecords.stream().filter(pr -> pr.getId().equals(id)).findAny();
  }

  private static RawRecord toRawRecord(SourceRecord sourceRecord) {
    return sourceRecord.getRawRecord();
  }

  private static ParsedRecord toParsedRecord(SourceRecord sourceRecord) {
    return sourceRecord.getParsedRecord();
  }

  private static List<SourceRecord> readSourceRecords() {
    File sourceRecordsDirectory = new File(SOURCE_RECORDS_FOLDER_PATH);
    String[] extensions = new String[] { "json" };
    return FileUtils.listFiles(sourceRecordsDirectory, extensions, false).stream()
      .map(TestMocks::readSourceRecord)
      .filter(sr -> sr.isPresent())
      .map(sr -> sr.get())
      .collect(Collectors.toList());
  }

  private static Optional<SourceRecord> readSourceRecord(File file) {
    try {
      SourceRecord sourceRecord = ObjectMapperTool.getDefaultMapper().readValue(file, SourceRecord.class);
      return Optional.of(sourceRecord.withParsedRecord(normalizeContent(sourceRecord.getParsedRecord())));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }

  private static List<Snapshot> readSnapshots(List<SourceRecord> sourceRecords) {
    return sourceRecords.stream()
      .map(TestMocks::readSnapshot)
      .filter(s -> s.isPresent())
      .map(s -> s.get())
      .distinct()
      .collect(Collectors.toList());
  }

  private static Optional<Snapshot> readSnapshot(SourceRecord sourceRecord) {
    File file = new File(format(SNAPSHOT_PATH_TEMPLATE, sourceRecord.getSnapshotId()));
    if (file.exists()) {
      try {
        Snapshot snapshot = ObjectMapperTool.getDefaultMapper().readValue(file, Snapshot.class);
        return Optional.of(snapshot);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return Optional.empty();
  }

  private static List<Record> readRecords(List<SourceRecord> sourceRecords) {
    return sourceRecords.stream()
      .map(TestMocks::readRecord)
      .filter(r -> r.isPresent())
      .map(r -> r.get())
      .distinct()
      .collect(Collectors.toList());
  }

  private static Optional<Record> readRecord(SourceRecord sourceRecord) {
    File file = new File(format(RECORD_PATH_TEMPLATE, sourceRecord.getRecordId()));
    if (file.exists()) {
      try {
        Record record = ObjectMapperTool.getDefaultMapper().readValue(file, Record.class)
          .withRawRecord(sourceRecord.getRawRecord())
          .withParsedRecord(sourceRecord.getParsedRecord())
          .withExternalIdsHolder(sourceRecord.getExternalIdsHolder())
          .withAdditionalInfo(sourceRecord.getAdditionalInfo());
        if (Objects.nonNull(sourceRecord.getMetadata())) {
          record.withMetadata(sourceRecord.getMetadata());
        }
        Optional<ErrorRecord> errorRecord = errorRecords.stream()
          .filter(er -> er.getId().equals(record.getId())).findAny();
        if (errorRecord.isPresent()) {
          record.withErrorRecord(errorRecord.get());
        }
        return Optional.of(record);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return Optional.empty();
  }

  private static List<ErrorRecord> readErrorRecords(List<SourceRecord> sourceRecords) {
    return sourceRecords.stream()
      .map(TestMocks::readErrorRecord)
      .filter(er -> er.isPresent())
      .map(er -> er.get())
      .distinct()
      .collect(Collectors.toList());
  }

  private static Optional<ErrorRecord> readErrorRecord(SourceRecord sourceRecord) {
    File file = new File(format(ERROR_RECORD_PATH_TEMPLATE, sourceRecord.getRecordId()));
    if (file.exists()) {
      try {
        ErrorRecord errorRecord = ObjectMapperTool.getDefaultMapper().readValue(file, ErrorRecord.class);
        errorRecord.withContent(sourceRecord.getParsedRecord().getContent());
        return Optional.of(errorRecord);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return Optional.empty();
  }

  public static ParsedRecord normalizeContent(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord.getContent()) && parsedRecord.getContent() instanceof LinkedHashMap) {
      parsedRecord.setContent(JsonObject.mapFrom(parsedRecord.getContent()).encode());
    }
    return parsedRecord;
  }

}