package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.RecordDao;
import org.folio.dao.SnapshotDao;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;

@Component
public class RecordServiceImpl implements RecordService {

  @Autowired
  private RecordDao recordDao;
  @Autowired
  private SnapshotDao snapshotDao;

  @Override
  public Future<RecordCollection> getRecords(String query, int offset, int limit, String tenantId) {
    return recordDao.getRecords(query, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return recordDao.getRecordById(id, tenantId);
  }

  @Override
  public Future<Boolean> saveRecord(Record record, String tenantId) {
    if (record.getId() == null) {
      record.setId(UUID.randomUUID().toString());
    }
    record.getRawRecord().setId(UUID.randomUUID().toString());
    if (record.getParsedRecord() != null) {
      record.getParsedRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getErrorRecord() != null) {
      record.getErrorRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getAdditionalInfo() == null) {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false));
    }
    return snapshotDao.getSnapshotById(record.getSnapshotId(), tenantId)
      .map(optionalRecordSnapshot -> optionalRecordSnapshot.orElseThrow(() -> new NotFoundException("Couldn't find snapshot with id " + record.getSnapshotId())))
      .compose(snapshot -> {
        if (snapshot.getProcessingStartedDate() == null) {
          return Future.failedFuture(new BadRequestException(
            String.format("Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s", snapshot.getStatus())));
        }
        return Future.succeededFuture();
      })
      .compose(f -> recordDao.calculateGeneration(record, tenantId))
      .compose(generation -> recordDao.saveRecord(record.withGeneration(generation), tenantId));
  }

  @Override
  public Future<Boolean> updateRecord(Record record, String tenantId) {
    return getRecordById(record.getId(), tenantId)
      .compose(optionalRecord -> optionalRecord
        .map(r -> {
          ParsedRecord parsedRecord = record.getParsedRecord();
          if (parsedRecord != null && (parsedRecord.getId() == null || parsedRecord.getId().isEmpty())) {
            parsedRecord.setId(UUID.randomUUID().toString());
          }
          ErrorRecord errorRecord = record.getErrorRecord();
          if (errorRecord != null && (errorRecord.getId() == null || errorRecord.getId().isEmpty())) {
            errorRecord.setId(UUID.randomUUID().toString());
          }
          return recordDao.updateRecord(record, tenantId);
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Record with id '%s' was not found", record.getId()))))
      );
  }

  @Override
  public Future<Boolean> deleteRecord(String id, String tenantId) {
    return recordDao.deleteRecord(id, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId) {
    return recordDao.getSourceRecords(query, offset, limit, deletedRecords, tenantId);
  }

}
