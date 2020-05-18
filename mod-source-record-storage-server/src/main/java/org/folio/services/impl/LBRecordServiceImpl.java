package org.folio.services.impl;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.AbstractEntityService;
import org.folio.services.LBRecordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;

@Service
public class LBRecordServiceImpl extends AbstractEntityService<Record, RecordCollection, RecordQuery, LBRecordDao>
    implements LBRecordService {

  @Autowired
  private LBSnapshotDao snapshotDao;

  @Autowired
  private ParsedRecordDao parsedRecordDao;

  @Override
  public Future<Record> save(Record record, String tenantId) {
    return validateSnapshotProcessing(record.getSnapshotId(), tenantId)
      .compose(v -> dao.calculateGeneration(record, tenantId))
      .compose(generation -> super.save(record.withGeneration(generation), tenantId));
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId) {
    List<Future> futures = recordCollection.getRecords().stream()
      .map(record -> save(record, tenantId))
      .collect(Collectors.toList());
    Promise<RecordsBatchResponse> promise = Promise.promise();
    CompositeFuture.join(futures).onComplete(ar -> {
      RecordsBatchResponse response = new RecordsBatchResponse();
      futures.forEach(save -> {
        if (save.failed()) {
          response.getErrorMessages().add(save.cause().getMessage());
        } else {
          response.getRecords().add((Record) save.result());
        }
      });
      response.setTotalRecords(response.getRecords().size());
      promise.complete(response);
    });
    return promise.future();
  }

  @Override
  public Future<Record> getFormattedRecord(String externalIdIdentifier, String id, String tenantId) {
    Future<Optional<Record>> future;
    switch (externalIdIdentifier) {
      case "instanceId": 
        future = dao.getByInstanceId(id, tenantId);
        break;
      default:
        future = dao.getById(id, tenantId);
        break;
    }
    return future.map(record -> record
      .orElseThrow(() -> new NotFoundException(String.format("Couldn't find record with %s %s", externalIdIdentifier, id))))
      .compose(record -> parsedRecordDao.getById(record.getId(), tenantId)
      .map(parsedRecord -> parsedRecord
        .orElseThrow(() -> new NotFoundException(String.format("Couldn't find parsed record with id %s", record.getId()))))
      .map(parsedRecord -> record.withParsedRecord(parsedRecord)));
  }

  private Future<Void> validateSnapshotProcessing(String snapshotId, String tenantId) {
    return snapshotDao.getById(snapshotId, tenantId)
      .map(snapshot -> snapshot
        .orElseThrow(() -> new NotFoundException(String.format("Couldn't find snapshot with id %s", snapshotId))))
      .compose(this::isProcessing);
  }

  private Future<Void> isProcessing(Snapshot snapshot) {
    if (Objects.isNull(snapshot.getProcessingStartedDate())) {
      String message = "Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s";
      return Future.failedFuture(new BadRequestException(String.format(message, snapshot.getStatus())));
    }
    return Future.succeededFuture();
  }

}