package org.folio.dao.impl;

import java.io.File;
import java.io.IOException;
import java.util.List;
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
import org.junit.BeforeClass;

import io.vertx.ext.unit.TestContext;

public abstract class DaoTestMocks {

  private static final String SOURCE_RECORDS_FOLDER_PATH = "src/test/resources/mock/sourceRecords";

  private static final String SNAPSHOT_PATH_TEMPLATE = "src/test/resources/mock/snapshots/%s.json";
  private static final String RECORD_PATH_TEMPLATE = "src/test/resources/mock/records/%s.json";
  private static final String ERROR_RECORD_PATH_TEMPLATE = "src/test/resources/mock/errorRecords/%s.json";

  private static List<Snapshot> snapshots;

  private static List<Record> records;

  private static List<RawRecord> rawRecords;

  private static List<ParsedRecord> parsedRecords;

  private static List<ErrorRecord> errorRecords;

  @BeforeClass
  public static void setUpMocks(final TestContext context) throws Exception {
    List<SourceRecord> sourceRecords = readSourceRecords();
    rawRecords = sourceRecords.stream().map(DaoTestMocks::toRawRecord).collect(Collectors.toList());
    parsedRecords = sourceRecords.stream().map(DaoTestMocks::toParsedRecord).collect(Collectors.toList());
    errorRecords = readErrorRecords(sourceRecords);
    records = readRecords(sourceRecords);
    snapshots = readSnapshots(sourceRecords);
  }

  protected List<Snapshot> getSnapshots() {
    return snapshots;
  }

  protected Snapshot getSnapshot(int index) {
    return snapshots.get(index);
  }

  protected List<Record> getRecords() {
    return records;
  }

  protected Record getRecord(int index) {
    return records.get(index);
  }

  protected List<ErrorRecord> getErrorRecords() {
    return errorRecords;
  }

  protected ErrorRecord getErrorRecord(int index) {
    return errorRecords.get(index);
  }

  protected List<RawRecord> getRawRecords() {
    return rawRecords;
  }

  protected RawRecord getRawRecord(int index) {
    return rawRecords.get(index);
  }

  protected List<ParsedRecord> getParsedRecords() {
    return parsedRecords;
  }

  protected ParsedRecord getParsedRecord(int index) {
    return parsedRecords.get(index);
  }

  protected Optional<RawRecord> getRawRecord(String id) {
    return rawRecords.stream().filter(rr -> rr.getId().equals(id)).findAny();
  }

  protected Optional<ParsedRecord> getParsedRecord(String id) {
    return parsedRecords.stream().filter(pr -> pr.getId().equals(id)).findAny();
  }

  private static RawRecord toRawRecord(SourceRecord sourceRecord) {
    return sourceRecord.getRawRecord();
  }

  private static ParsedRecord toParsedRecord(SourceRecord sourceRecord) {
    ParsedRecord parsedRecord = new ParsedRecord()
      .withId(sourceRecord.getParsedRecord().getId())
      .withFormattedContent(sourceRecord.getParsedRecord().getFormattedContent());
    Optional<String> content = getContent(sourceRecord);
    if (content.isPresent()) {
      parsedRecord.withContent(content.get());
    }
    return parsedRecord;
  }

  private static List<SourceRecord> readSourceRecords() {
    File sourceRecordsDirectory = new File(SOURCE_RECORDS_FOLDER_PATH);
    String[] extensions = new String[] { "json" };
    return FileUtils.listFiles(sourceRecordsDirectory, extensions, false).stream()
      .map(DaoTestMocks::readSourceRecord)
      .filter(sr -> sr.isPresent())
      .map(sr -> sr.get())
      .collect(Collectors.toList());
  }

  private static Optional<SourceRecord> readSourceRecord(File file) {
    try {
      return Optional.of(ObjectMapperTool.getDefaultMapper().readValue(file, SourceRecord.class));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }


  private static List<Snapshot> readSnapshots(List<SourceRecord> sourceRecords) {
    return sourceRecords.stream()
      .map(DaoTestMocks::readSnapshot)
      .filter(s -> s.isPresent())
      .map(s -> s.get())
      .distinct()
      .collect(Collectors.toList());
  }

  private static Optional<Snapshot> readSnapshot(SourceRecord sourceRecord) {
    File file = new File(String.format(SNAPSHOT_PATH_TEMPLATE, sourceRecord.getSnapshotId()));
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
      .map(DaoTestMocks::readRecord)
      .filter(r -> r.isPresent())
      .map(r -> r.get())
      .distinct()
      .collect(Collectors.toList());
  }

  private static Optional<Record> readRecord(SourceRecord sourceRecord) {
    File file = new File(String.format(RECORD_PATH_TEMPLATE, sourceRecord.getRecordId()));
    if (file.exists()) {
      try {
        Record record = ObjectMapperTool.getDefaultMapper().readValue(file, Record.class)
          .withRawRecord(sourceRecord.getRawRecord())
          .withParsedRecord(sourceRecord.getParsedRecord())
          .withExternalIdsHolder(sourceRecord.getExternalIdsHolder())
          .withAdditionalInfo(sourceRecord.getAdditionalInfo());
        if (sourceRecord.getMetadata() != null) {
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
      .map(DaoTestMocks::readErrorRecord)
      .filter(er -> er.isPresent())
      .map(er -> er.get())
      .distinct()
      .collect(Collectors.toList());
  }

  private static Optional<ErrorRecord> readErrorRecord(SourceRecord sourceRecord) {
    File file = new File(String.format(ERROR_RECORD_PATH_TEMPLATE, sourceRecord.getRecordId()));
    if (file.exists()) {
      try {
        ErrorRecord errorRecord = ObjectMapperTool.getDefaultMapper().readValue(file, ErrorRecord.class);
        Optional<String> content = getContent(sourceRecord);
        if (content.isPresent()) {
          errorRecord.withContent(content.get());
        }
        return Optional.of(errorRecord);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return Optional.empty();
  }

  private static Optional<String> getContent(SourceRecord sourceRecord) {
    try {
      return Optional.of(ObjectMapperTool.getDefaultMapper().writeValueAsString(sourceRecord.getParsedRecord().getContent()));
    } catch (IOException e) {
      e.printStackTrace();
    }
    return Optional.empty();
  }

}