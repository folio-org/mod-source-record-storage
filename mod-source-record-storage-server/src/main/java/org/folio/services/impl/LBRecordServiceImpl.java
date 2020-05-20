package org.folio.services.impl;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;
import org.folio.services.AbstractEntityService;
import org.folio.services.LBRecordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.SqlConnection;

@Service
public class LBRecordServiceImpl extends AbstractEntityService<Record, RecordCollection, RecordQuery, LBRecordDao>
    implements LBRecordService {

  @Autowired
  private LBSnapshotDao snapshotDao;

  @Autowired
  private RawRecordDao rawRecordDao;

  @Autowired
  private ParsedRecordDao parsedRecordDao;

  @Autowired
  private ErrorRecordDao errorRecordDao;

  @Override
  public Future<Record> save(Record record, String tenantId) {
    if (StringUtils.isEmpty(record.getId())) {
      record.setId(UUID.randomUUID().toString());
    }
    if (StringUtils.isEmpty(record.getMatchedId())) {
      record.setMatchedId(record.getId());
    }
    if (Objects.nonNull(record.getParsedRecord()) && StringUtils.isEmpty(record.getParsedRecord().getId())) {
      record.getParsedRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getErrorRecord()) && StringUtils.isEmpty(record.getErrorRecord().getId())) {
      record.getErrorRecord().setId(record.getId());
    }
    if (Objects.isNull(record.getAdditionalInfo())) {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false));
    }
    return dao.inTransaction(tenantId, connection -> validateSnapshotProcessing(connection, record.getSnapshotId(), tenantId)
      .compose(v -> dao.calculateGeneration(connection, record, tenantId))
      .compose(generation -> insertOrUpdateRecord(connection, record.withGeneration(generation), tenantId)));
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
  public Future<Record> update(Record record, String tenantId) {
    return dao.inTransaction(tenantId, connection -> dao.getById(connection, record.getId(), tenantId)
      .compose(optionalRecord -> optionalRecord
        .map(r -> {
          if (Objects.nonNull(record.getParsedRecord()) && StringUtils.isEmpty(record.getParsedRecord().getId())) {
            record.getParsedRecord().setId(record.getId());
          }
          if (Objects.nonNull(record.getErrorRecord()) && StringUtils.isEmpty(record.getErrorRecord().getId())) {
            record.getErrorRecord().setId(record.getId());
          }
          return insertOrUpdateRecord(connection, record, tenantId);
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Record with id '%s' was not found", record.getId())))))
      );
  }

  @Override
  public Future<Record> getFormattedRecord(String externalIdIdentifier, String id, String tenantId) {
    // TODO: this should use SQL functions to get partial record with parsed record
    Future<Optional<Record>> future;
    if (StringUtils.isNotEmpty(externalIdIdentifier)) {
      ExternalIdType externalIdType = getExternalIdType(externalIdIdentifier);
      future = dao.getRecordByExternalId(id, externalIdType, tenantId);
    } else {
      future = dao.getById(id, tenantId);
    }
    return future.map(record -> record
      .orElseThrow(() -> new NotFoundException(String.format("Couldn't find record with %s %s", externalIdIdentifier, id))))
      .compose(record -> parsedRecordDao.getById(record.getId(), tenantId)
      .map(parsedRecord -> parsedRecord
        .orElseThrow(() -> new NotFoundException(String.format("Couldn't find parsed record with id %s", record.getId()))))
      .map(parsedRecord -> getRecordWithParsedRecord(record, parsedRecord)));
  }

  @Override
  public Future<Record> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId) {
    String id = suppressFromDiscoveryDto.getId();
    IncomingIdType idType = suppressFromDiscoveryDto.getIncomingIdType();
    Boolean suppressDiscovery = suppressFromDiscoveryDto.getSuppressFromDiscovery();
    return dao.inTransaction(tenantId, connection ->
      dao.getRecordById(connection, id, idType, tenantId)
        .map(record -> record.orElseThrow(() -> new NotFoundException(String.format("Couldn't find record with %s %s", idType, id))))
        .map(record -> record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(suppressDiscovery)))
        .compose(record -> dao.update(connection, record, tenantId)));
  }
  @Override
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId) {
    String id = parsedRecordDto.getId();
    // no processing of the record is performed apart from the update itself
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(snapshotId)
      .withStatus(Snapshot.Status.COMMITTED);
    return dao.inTransaction(tenantId, connection -> 
      dao.getById(connection, id, tenantId)
        .compose(optionalRecord -> optionalRecord
          .map(existingRecord -> snapshotDao.save(connection, snapshot, tenantId)
            .compose(s -> {
              Record newRecord = new Record()
                .withId(UUID.randomUUID().toString())
                .withSnapshotId(s.getJobExecutionId())
                .withMatchedId(parsedRecordDto.getId())
                .withRecordType(Record.RecordType.fromValue(parsedRecordDto.getRecordType().value()))
                .withParsedRecord(parsedRecordDto.getParsedRecord().withId(UUID.randomUUID().toString()))
                .withExternalIdsHolder(parsedRecordDto.getExternalIdsHolder())
                .withAdditionalInfo(parsedRecordDto.getAdditionalInfo())
                .withMetadata(parsedRecordDto.getMetadata())
                .withRawRecord(existingRecord.getRawRecord())
                .withOrder(existingRecord.getOrder())
                .withGeneration(existingRecord.getGeneration() + 1)
                .withState(Record.State.ACTUAL);
              return saveUpdatedRecord(connection, newRecord, existingRecord.withState(Record.State.OLD), tenantId);
            }))
          .orElse(Future.failedFuture(new NotFoundException(
            String.format("Record with id '%s' was not found", parsedRecordDto.getId()))))));
  }

  private Record getRecordWithParsedRecord(Record record, ParsedRecord parsedRecord) {
    return record.withParsedRecord(parsedRecord);
  }

  private Future<Record> insertOrUpdateRecord(SqlConnection connection, Record record, String tenantId) {
    return rawRecordDao.save(connection, record.getRawRecord(), tenantId)
        .compose(rr -> {
          if (Objects.nonNull(record.getParsedRecord())) {
            return insertOrUpdateParsedRecord(connection, record, tenantId);
          }
          return Future.succeededFuture();
        })
        .compose(succeeded -> {
          if (Boolean.FALSE.equals(succeeded) && Objects.nonNull(record.getErrorRecord())) {
            return errorRecordDao.save(connection, record.getErrorRecord(), tenantId);
          }
          return Future.succeededFuture();
        })
        .compose(er -> dao.save(connection, record, tenantId));
  }

  private Future<Boolean> insertOrUpdateParsedRecord(SqlConnection connection, Record record, String tenantId) {
    Promise<Boolean> promise = Promise.promise();
    ParsedRecord parsedRecord = record.getParsedRecord();
    parsedRecordDao.save(connection, parsedRecord, tenantId).onComplete(ar -> {
      if (ar.succeeded()) {
        record.withParsedRecord(ar.result());
      } else {
        ErrorRecord error = new ErrorRecord()
          .withId(record.getId())
          .withDescription(ar.cause().getMessage())
          .withContent(parsedRecord.getContent());
        record
          .withParsedRecord(null)
          .withErrorRecord(error);
      }
      promise.complete(ar.succeeded());
    });
    return promise.future();
  }

  private Future<Record> saveUpdatedRecord(SqlConnection connection, Record newRecord, Record oldRecord, String tenantId) {
    return insertOrUpdateRecord(connection, oldRecord, tenantId).compose(r -> insertOrUpdateRecord(connection, newRecord, tenantId));
  }

  private Future<Void> validateSnapshotProcessing(SqlConnection connection, String snapshotId, String tenantId) {
    return snapshotDao.getById(connection, snapshotId, tenantId)
      .map(snapshot -> snapshot.orElseThrow(() -> new NotFoundException(String.format("Couldn't find snapshot with id %s", snapshotId))))
      .compose(this::isProcessing);
  }

  private Future<Void> isProcessing(Snapshot snapshot) {
    if (Objects.isNull(snapshot.getProcessingStartedDate())) {
      String message = "Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s";
      return Future.failedFuture(new BadRequestException(String.format(message, snapshot.getStatus())));
    }
    return Future.succeededFuture();
  }

  private ExternalIdType getExternalIdType(String externalIdIdentifier) {
    try {
      return ExternalIdType.valueOf(externalIdIdentifier);
    } catch (IllegalArgumentException e) {
      String message = "The external Id type: %s is wrong.";
      throw new BadRequestException(String.format(message, externalIdIdentifier));
    }
  }

}