package org.folio.services.impl;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;
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
  private ParsedRecordDao parsedRecordDao;

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

  @Override
  public Future<Optional<Record>> getRecordById(String id, IncomingIdType idType, String tenantId) {
    return dao.getRecordById(id, idType, tenantId);
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId) {
    return dao.updateSuppressFromDiscoveryForRecord(suppressFromDiscoveryDto, tenantId);
  }

  @Override
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId) {
    return dao.updateSourceRecord(parsedRecordDto, snapshotId, tenantId);
  }

}