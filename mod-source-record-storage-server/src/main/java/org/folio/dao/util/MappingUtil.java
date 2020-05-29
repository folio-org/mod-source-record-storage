package org.folio.dao.util;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.Snapshot.Status;
import org.folio.rest.jooq.enums.JobExecutionStatus;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.rest.jooq.enums.RecordType;
import org.folio.rest.jooq.tables.pojos.ErrorRecordsLb;
import org.folio.rest.jooq.tables.pojos.MarcRecordsLb;
import org.folio.rest.jooq.tables.pojos.RawRecordsLb;
import org.folio.rest.jooq.tables.pojos.RecordsLb;
import org.folio.rest.jooq.tables.pojos.SnapshotsLb;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class MappingUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MappingUtil.class);

  private MappingUtil() { }

  public static Snapshot fromDatabase(SnapshotsLb snapshotLb) {
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(snapshotLb.getId().toString())
      .withStatus(Status.fromValue(snapshotLb.getStatus().toString()));
    if (Objects.nonNull(snapshotLb.getProcessingStartedDate())) {
      snapshot.withProcessingStartedDate(Date.from(snapshotLb.getProcessingStartedDate().toInstant()));
    }
    Metadata metadata = new Metadata();
    if (Objects.nonNull(snapshotLb.getCreatedByUserId())) {
      metadata.withCreatedByUserId(snapshotLb.getCreatedByUserId().toString());
    }
    if (Objects.nonNull(snapshotLb.getCreatedDate())) {
      metadata.withCreatedDate(Date.from(snapshotLb.getCreatedDate().toInstant()));
    }
    if (Objects.nonNull(snapshotLb.getUpdatedByUserId())) {
      metadata.withUpdatedByUserId(snapshotLb.getUpdatedByUserId().toString());
    }
    if (Objects.nonNull(snapshotLb.getUpdatedDate())) {
      metadata.withUpdatedDate(Date.from(snapshotLb.getUpdatedDate().toInstant()));
    }
    return snapshot.withMetadata(metadata);
  }

  public static List<Snapshot> snapshotsFromDatabase(List<SnapshotsLb> snapshots) {
    return snapshots.stream().map(MappingUtil::fromDatabase).collect(Collectors.toList());
  }

  public static SnapshotsLb toDatabase(Snapshot snapshot) {
    SnapshotsLb snapshotLb = new SnapshotsLb();
    if (StringUtils.isNotEmpty(snapshot.getJobExecutionId())) {
      snapshotLb.setId(UUID.fromString(snapshot.getJobExecutionId()));
    }
    if (Objects.nonNull(snapshot.getStatus())) {
      snapshotLb.setStatus(JobExecutionStatus.valueOf(snapshot.getStatus().toString()));
    }
    if (Objects.nonNull(snapshot.getProcessingStartedDate())) {
      snapshotLb.setProcessingStartedDate(snapshot.getProcessingStartedDate().toInstant().atOffset(ZoneOffset.UTC));
    }
    if (Objects.nonNull(snapshot.getMetadata())) {
      if (Objects.nonNull(snapshot.getMetadata().getCreatedByUserId())) {
        snapshotLb.setCreatedByUserId(UUID.fromString(snapshot.getMetadata().getCreatedByUserId()));
      }
      if (Objects.nonNull(snapshot.getMetadata().getCreatedDate())) {
        snapshotLb.setCreatedDate(snapshot.getMetadata().getCreatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
      if (Objects.nonNull(snapshot.getMetadata().getUpdatedByUserId())) {
        snapshotLb.setUpdatedByUserId(UUID.fromString(snapshot.getMetadata().getUpdatedByUserId()));
      }
      if (Objects.nonNull(snapshot.getMetadata().getUpdatedDate())) {
        snapshotLb.setUpdatedDate(snapshot.getMetadata().getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
    }
    return snapshotLb;
  }

  public static List<SnapshotsLb> snapshotsToDatabase(List<Snapshot> snapshots) {
    return snapshots.stream().map(MappingUtil::toDatabase).collect(Collectors.toList());
  }

  public static Record recordFromDatabase(RecordsLb recordLb) {
    Record record = new Record()
      .withId(recordLb.getId().toString())
      .withSnapshotId(recordLb.getSnapshotId().toString())
      .withMatchedId(recordLb.getMatchedId().toString())
      .withRecordType(org.folio.rest.jaxrs.model.Record.RecordType.valueOf(recordLb.getRecordType().toString()))
      .withState(org.folio.rest.jaxrs.model.Record.State.valueOf(recordLb.getState().toString()))
      .withOrder(recordLb.getOrderInFile())
      .withGeneration(recordLb.getGeneration());
      AdditionalInfo additionalInfo = new AdditionalInfo();
      if (Objects.nonNull(recordLb.getSuppressDiscovery())) {
        additionalInfo.withSuppressDiscovery(recordLb.getSuppressDiscovery());
      }
      ExternalIdsHolder externalIdsHolder = new ExternalIdsHolder();
      if (Objects.nonNull(recordLb.getInstanceId())) {
        externalIdsHolder.withInstanceId(recordLb.getInstanceId().toString());
      }
      Metadata metadata = new Metadata();
      if (Objects.nonNull(recordLb.getCreatedByUserId())) {
        metadata.withCreatedByUserId(recordLb.getCreatedByUserId().toString());
      }
      if (Objects.nonNull(recordLb.getCreatedDate())) {
        metadata.withCreatedDate(Date.from(recordLb.getCreatedDate().toInstant()));
      }
      if (Objects.nonNull(recordLb.getUpdatedByUserId())) {
        metadata.withUpdatedByUserId(recordLb.getUpdatedByUserId().toString());
      }
      if (Objects.nonNull(recordLb.getUpdatedDate())) {
        metadata.withUpdatedDate(Date.from(recordLb.getUpdatedDate().toInstant()));
      }
      return record
        .withAdditionalInfo(additionalInfo)
        .withExternalIdsHolder(externalIdsHolder)
        .withMetadata(metadata);
  }

  public static RecordsLb recordToDatabase(Record record) {
    RecordsLb recordLb = new RecordsLb();
    if (StringUtils.isNotEmpty(record.getId())) {
      recordLb.setId(UUID.fromString(record.getId()));
    }
    if (StringUtils.isNotEmpty(record.getSnapshotId())) {
      recordLb.setSnapshotId(UUID.fromString(record.getSnapshotId()));
    }
    if (StringUtils.isNotEmpty(record.getMatchedId())) {
      recordLb.setMatchedId(UUID.fromString(record.getMatchedId()));
    }
    if (Objects.nonNull(record.getRecordType())) {
      recordLb.setRecordType(RecordType.valueOf(record.getRecordType().toString()));
    }
    if (Objects.nonNull(record.getState())) {
      recordLb.setState(RecordState.valueOf(record.getState().toString()));
    }
    recordLb.setOrderInFile(record.getOrder());
    recordLb.setGeneration(record.getGeneration());
    if (Objects.nonNull(record.getAdditionalInfo())) {
      recordLb.setSuppressDiscovery(record.getAdditionalInfo().getSuppressDiscovery());
    }
    if (Objects.nonNull(record.getExternalIdsHolder())) {
      if (StringUtils.isNotEmpty(record.getExternalIdsHolder().getInstanceId())) {
        recordLb.setInstanceId(UUID.fromString(record.getExternalIdsHolder().getInstanceId()));
      }
    }
    if (Objects.nonNull(record.getMetadata())) {
      if (Objects.nonNull(record.getMetadata().getCreatedByUserId())) {
        recordLb.setCreatedByUserId(UUID.fromString(record.getMetadata().getCreatedByUserId()));
      }
      if (Objects.nonNull(record.getMetadata().getCreatedDate())) {
        recordLb.setCreatedDate(record.getMetadata().getCreatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
      if (Objects.nonNull(record.getMetadata().getUpdatedByUserId())) {
        recordLb.setUpdatedByUserId(UUID.fromString(record.getMetadata().getUpdatedByUserId()));
      }
      if (Objects.nonNull(record.getMetadata().getUpdatedDate())) {
        recordLb.setUpdatedDate(record.getMetadata().getUpdatedDate().toInstant().atOffset(ZoneOffset.UTC));
      }
    }
    return recordLb;
  }

  public static List<Record> recordsFromDatabase(List<RecordsLb> records) {
    return records.stream().map(MappingUtil::recordFromDatabase).collect(Collectors.toList());
  }

  public static List<RecordsLb> recordsToDatabase(List<Record> records) {
    return records.stream().map(MappingUtil::recordToDatabase).collect(Collectors.toList());
  }

  public static RawRecord rawRecordFromDatabase(RawRecordsLb record) {
    return new RawRecord()
      .withId(record.getId().toString())
      .withContent(record.getContent());
  }

  public static RawRecordsLb rawRecordToDatabase(RawRecord record) {
    RawRecordsLb recordLb = new RawRecordsLb();
    if (StringUtils.isNotEmpty(record.getId())) {
      recordLb.setId(UUID.fromString(record.getId()));
    }
    recordLb.setContent(record.getContent());
    return recordLb;
  }

  public static List<RawRecord> rawRecordsFromDatabase(List<RawRecordsLb> records) {
    return records.stream().map(MappingUtil::rawRecordFromDatabase).collect(Collectors.toList());
  }

  public static List<RawRecordsLb> rawRecordsToDatabase(List<RawRecord> records) {
    return records.stream().map(MappingUtil::rawRecordToDatabase).collect(Collectors.toList());
  }

  public static ParsedRecord parsedRecordFromDatabase(MarcRecordsLb record) {
    return new ParsedRecord()
      .withId(record.getId().toString())
      .withContent(record.getContent());
  }

  public static MarcRecordsLb parsedRecordToDatabase(ParsedRecord record) {
    MarcRecordsLb recordLb = new MarcRecordsLb();
    if (StringUtils.isNotEmpty(record.getId())) {
      recordLb.setId(UUID.fromString(record.getId()));
    }
    if (Objects.nonNull(record.getContent())) {
      recordLb.setContent((String) record.getContent());
    }
    return recordLb;
  }

  public static List<ParsedRecord> parsedRecordsFromDatabase(List<MarcRecordsLb> records) {
    return records.stream().map(MappingUtil::parsedRecordFromDatabase).collect(Collectors.toList());
  }

  public static List<MarcRecordsLb> parsedRecordsToDatabase(List<ParsedRecord> records) {
    return records.stream().map(MappingUtil::parsedRecordToDatabase).collect(Collectors.toList());
  }

  public static ErrorRecord errorRecordFromDatabase(ErrorRecordsLb record) {
    return new ErrorRecord()
      .withId(record.getId().toString())
      .withContent(record.getContent())
      .withDescription(record.getDescription());
  }

  public static ErrorRecordsLb errorRecordToDatabase(ErrorRecord record) {
    ErrorRecordsLb recordLb = new ErrorRecordsLb();
    if (StringUtils.isNotEmpty(record.getId())) {
      recordLb.setId(UUID.fromString(record.getId()));
    }
    if (Objects.nonNull(record.getContent())) {
      recordLb.setContent((String) record.getContent());
    }
    recordLb.setDescription(record.getDescription());
    return recordLb;
  }

  public static List<ErrorRecord> errorRecordsFromDatabase(List<ErrorRecordsLb> records) {
    return records.stream().map(MappingUtil::errorRecordFromDatabase).collect(Collectors.toList());
  }

  public static List<ErrorRecordsLb> errorRecordsToDatabase(List<ErrorRecord> records) {
    return records.stream().map(MappingUtil::errorRecordToDatabase).collect(Collectors.toList());
  }

  /**
   * Format parsed record content
   * 
   * @param parsedRecord parsed record
   * @return parsed record with formatted content
   */
  public static ParsedRecord formatContent(ParsedRecord parsedRecord) {
    if (Objects.nonNull(parsedRecord.getContent())) {
      try {
        String formattedContent = MarcUtil.marcJsonToTxtMarc((String) parsedRecord.getContent());
        parsedRecord.withFormattedContent(formattedContent);
      } catch (IOException e) {
        LOG.error("Error formatting content", e);
      }
    }
    return parsedRecord;
  }

}