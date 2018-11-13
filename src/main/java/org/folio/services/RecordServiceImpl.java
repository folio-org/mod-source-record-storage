package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.RecordDao;
import org.folio.dao.RecordDaoImpl;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class RecordServiceImpl implements RecordService {

  private RecordDao recordDao;

  public RecordServiceImpl(Vertx vertx, String tenantId) {
    this.recordDao = new RecordDaoImpl(vertx, tenantId);
  }

  @Override
  public Future<List<Record>> getRecords(String query, int offset, int limit) {
    return recordDao.getRecords(query, offset, limit);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id) {
    return recordDao.getRecordById(id);
  }

  @Override
  public Future<Boolean> saveRecord(Record record) {
    record.setId(UUID.randomUUID().toString());
    record.getSourceRecord().setId(UUID.randomUUID().toString());
    if (record.getParsedRecord() != null) {
      record.getParsedRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getErrorRecord() != null) {
      record.getErrorRecord().setId(UUID.randomUUID().toString());
    }
    return recordDao.saveRecord(record);
  }


  @Override
  public Future<Boolean> updateRecord(Record record) {
    return getRecordById(record.getId())
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
          return recordDao.updateRecord(record);
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Record with id '%s' was not found", record.getId()))))
      );
  }

  @Override
  public Future<Boolean> deleteRecord(String id) {
    return recordDao.deleteRecord(id);
  }

}
