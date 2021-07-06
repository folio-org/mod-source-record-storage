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

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.FileUtils;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;

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
    return new ArrayList<>(snapshots.stream().map(TestMocks::clone).collect(Collectors.toList()));
  }

  public static Snapshot getSnapshot(int index) {
    return clone(snapshots.get(index));
  }

  public static List<Record> getRecords() {
    return new ArrayList<>(records.stream().map(TestMocks::clone).collect(Collectors.toList()));
  }

  public static Record getRecord(int index) {
    return clone(records.get(index));
  }

  public static Record getRecords(RecordType type) {
    return records.stream()
      .filter(s -> s.getRecordType().equals(type))
      .map(TestMocks::clone)
      .findFirst()
      .get();
  }

  public static Record getMarcBibRecord() {
    return getRecords(RecordType.MARC_BIB);
  }

  public static Record getMarcAuthorityRecord() {
    return getRecords(RecordType.MARC_AUTHORITY);
  }

  public static Record getEdifactRecord() {
    return getRecords(RecordType.EDIFACT);
  }

  public static List<ErrorRecord> getErrorRecords() {
    return new ArrayList<>(errorRecords.stream().map(TestMocks::clone).collect(Collectors.toList()));
  }

  public static ErrorRecord getErrorRecord(int index) {
    return clone(errorRecords.get(index));
  }

  public static List<RawRecord> getRawRecords() {
    return new ArrayList<>(rawRecords.stream().map(TestMocks::clone).collect(Collectors.toList()));
  }

  public static RawRecord getRawRecord(int index) {
    return clone(rawRecords.get(index));
  }

  public static List<ParsedRecord> getParsedRecords() {
    return new ArrayList<>(parsedRecords.stream().map(TestMocks::clone).collect(Collectors.toList()));
  }

  public static ParsedRecord getParsedRecord(int index) {
    return clone(parsedRecords.get(index));
  }

  public static Optional<Snapshot> getSnapshot(String id) {
    return snapshots.stream().map(TestMocks::clone).filter(s -> s.getJobExecutionId().equals(id)).findAny();
  }

  public static Optional<Record> getRecord(String id) {
    return records.stream().map(TestMocks::clone).filter(r -> r.getId().equals(id)).findAny();
  }

  public static Optional<ErrorRecord> getErrorRecord(String id) {
    return errorRecords.stream().map(TestMocks::clone).filter(er -> er.getId().equals(id)).findAny();
  }

  public static Optional<RawRecord> getRawRecord(String id) {
    return rawRecords.stream().map(TestMocks::clone).filter(rr -> rr.getId().equals(id)).findAny();
  }

  public static Optional<ParsedRecord> getParsedRecord(String id) {
    return parsedRecords.stream().map(TestMocks::clone).filter(pr -> pr.getId().equals(id)).findAny();
  }

  public static ParsedRecord normalizeContent(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord.getContent()) && parsedRecord.getContent() instanceof LinkedHashMap) {
      parsedRecord.setContent(JsonObject.mapFrom(parsedRecord.getContent()).encode());
    }
    return parsedRecord;
  }

  private static RawRecord toRawRecord(SourceRecord sourceRecord) {
    return sourceRecord.getRawRecord();
  }

  private static ParsedRecord toParsedRecord(SourceRecord sourceRecord) {
    return sourceRecord.getParsedRecord();
  }

  private static List<SourceRecord> readSourceRecords() {
    File sourceRecordsDirectory = new File(SOURCE_RECORDS_FOLDER_PATH);
    String[] extensions = new String[]{ "json" };
    return FileUtils.listFiles(sourceRecordsDirectory, extensions, false).stream()
      .map(TestMocks::readSourceRecord)
      .filter(sr -> sr.isPresent())
      .map(sr -> sr.get())
      .collect(Collectors.toList());
  }

  private static Optional<SourceRecord> readSourceRecord(File file) {
    try {
      SourceRecord sourceRecord = new ObjectMapper().readValue(file, SourceRecord.class);
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
        Snapshot snapshot = new ObjectMapper().readValue(file, Snapshot.class);
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
        Record record = new ObjectMapper().readValue(file, Record.class)
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
        ErrorRecord errorRecord = new ObjectMapper().readValue(file, ErrorRecord.class);
        errorRecord.withContent(sourceRecord.getParsedRecord().getContent());
        return Optional.of(errorRecord);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return Optional.empty();
  }

  private static Snapshot clone(Snapshot snapshot) {
    return new Snapshot()
      .withJobExecutionId(snapshot.getJobExecutionId())
      .withStatus(snapshot.getStatus())
      .withProcessingStartedDate(snapshot.getProcessingStartedDate())
      .withMetadata(snapshot.getMetadata());
  }

  private static Record clone(Record record) {
    return new Record()
      .withId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withMatchedId(record.getMatchedId())
      .withState(record.getState())
      .withRecordType(record.getRecordType())
      .withOrder(record.getOrder())
      .withGeneration(record.getGeneration())
      .withLeaderRecordStatus(record.getLeaderRecordStatus())
      .withRawRecord(clone(record.getRawRecord()))
      .withParsedRecord(clone(record.getParsedRecord()))
      .withErrorRecord(clone(record.getErrorRecord()))
      .withExternalIdsHolder(clone(record.getExternalIdsHolder()))
      .withAdditionalInfo(clone(record.getAdditionalInfo()))
      .withMetadata(clone(record.getMetadata()));
  }

  private static RawRecord clone(RawRecord rawRecord) {
    if (Objects.nonNull(rawRecord)) {
      return new RawRecord()
        .withId(rawRecord.getId())
        .withContent(rawRecord.getContent());
    }
    return null;
  }

  private static ParsedRecord clone(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord)) {
      return new ParsedRecord()
        .withId(parsedRecord.getId())
        .withContent(parsedRecord.getContent());
    }
    return null;
  }

  private static ErrorRecord clone(ErrorRecord errorRecord) {
    if (Objects.nonNull(errorRecord)) {
      return new ErrorRecord()
        .withId(errorRecord.getId())
        .withDescription(errorRecord.getDescription())
        .withContent(errorRecord.getContent());
    }
    return null;
  }

  private static ExternalIdsHolder clone(ExternalIdsHolder externalIdsHolder) {
    if (Objects.nonNull(externalIdsHolder)) {
      return new ExternalIdsHolder()
        .withInstanceId(externalIdsHolder.getInstanceId())
        .withInstanceHrid(externalIdsHolder.getInstanceHrid());
    }
    return null;
  }

  private static AdditionalInfo clone(AdditionalInfo additionalInfo) {
    if (Objects.nonNull(additionalInfo)) {
      return new AdditionalInfo()
        .withSuppressDiscovery(additionalInfo.getSuppressDiscovery());
    }
    return null;
  }

  private static Metadata clone(Metadata metadata) {
    if (Objects.nonNull(metadata)) {
      return new Metadata()
        .withCreatedByUserId(metadata.getCreatedByUserId())
        .withCreatedByUsername(metadata.getCreatedByUsername())
        .withCreatedDate(metadata.getCreatedDate())
        .withUpdatedByUserId(metadata.getUpdatedByUserId())
        .withUpdatedByUsername(metadata.getUpdatedByUsername())
        .withUpdatedDate(metadata.getUpdatedDate());
    }
    return null;
  }

}
