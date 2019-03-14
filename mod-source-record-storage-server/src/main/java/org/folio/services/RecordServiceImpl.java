package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.dao.RecordDao;
import org.folio.dao.SnapshotDao;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
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
    return calculateGeneration(record, tenantId)
      .compose(r -> recordDao.saveRecord(r, tenantId));
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
  public Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, String tenantId) {
    return recordDao.getSourceRecords(query, offset, limit, tenantId);
  }

  /**
   * Incrementes generation in case a record with the same matchedId exists
   * and the snapshot it is linked to is COMMITTED before the processing of the current one started
   *
   * @param record   - record
   * @param tenantId - tenant id
   * @return record with generation set wrapped in future
   */
  private Future<Record> calculateGeneration(Record record, String tenantId) {
    Future<Record> future = Future.future();
    return recordDao.getRecords("matchedId=" + record.getMatchedId(), 0, Integer.MAX_VALUE, tenantId)
      .compose(collection -> {
        if (collection.getRecords().isEmpty()) {
          return Future.succeededFuture(0);
        } else {
          MutableInt generation = new MutableInt();
          return snapshotDao.getSnapshotById(record.getSnapshotId(), tenantId)
            .map(optionalRecordSnapshot -> optionalRecordSnapshot.orElseThrow(() -> new NotFoundException("Couldn't find snapshot with id " + record.getSnapshotId())))
            .compose(recordSnapshot -> {
              List<Future> futures = new ArrayList<>();
              collection.getRecords().forEach(r -> futures.add(snapshotDao.getSnapshotById(r.getSnapshotId(), tenantId)
                .map(optionalSnapshot -> optionalSnapshot.orElseThrow(() -> new NotFoundException("Couldn't find snapshot with id " + r.getSnapshotId())))
                .compose(snapshot -> {
                  if (Snapshot.Status.COMMITTED.equals(snapshot.getStatus())
                    && snapshot.getMetadata().getUpdatedDate().before(recordSnapshot.getMetadata().getUpdatedDate())) {
                    generation.increment();
                  }
                  return Future.succeededFuture();
                }))
              );
              return CompositeFuture.all(futures);
            })
            .compose(compositeFuture -> Future.succeededFuture(generation.getValue()));
        }
      }).compose(generation -> {
        future.complete(record.withGeneration(generation));
        return future;
      });
  }

}
