package org.folio.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.dao.RecordDao;
import org.folio.dao.SnapshotDao;
import org.folio.rest.jaxrs.model.*;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Component
public class RecordServiceImpl implements RecordService {

  private static final Logger LOG = LoggerFactory.getLogger(RecordServiceImpl.class);
  public static final String PROCESSING_START_DATE_IS_NOT_SET_MSG = "Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s";

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
    return checkAssociationWithSnapshot(initFields(record), tenantId)
      .compose(it -> fillGeneration(it, tenantId))
      .compose(it -> recordDao.saveRecord(it, tenantId));
  }

  private Future<Record> checkAssociationWithSnapshot(Record record, String tenantId) {
    return snapshotDao.getSnapshotById(record.getSnapshotId(), tenantId)
      .map(Optional::get)
      .compose(snapshot -> validateSnapshot(snapshot, record));
  }

  private Future<Record> fillGeneration(Record record, String tenantId) {
    return recordDao.calculateGeneration(record, tenantId)
      .map(record::withGeneration);
  }

  private Future<Record> validateSnapshot(Snapshot snapshot, Record record) {
    Future<Record> future = Future.future();
    if (isNull(snapshot)) {
      future.fail(new NotFoundException("Couldn't find snapshot with id " + record.getSnapshotId()));
    }
    if (nonNull(snapshot) && isNull(snapshot.getProcessingStartedDate())) {
      future.fail(new BadRequestException(format(PROCESSING_START_DATE_IS_NOT_SET_MSG, snapshot.getStatus())));
    }
    return future.isComplete() ? future : Future.succeededFuture(record);
  }

  private Record initFields(Record record) {
    if (record.getId() == null) {
      record.setId(UUID.randomUUID().toString());
    }
    if (record.getRawRecord() != null && record.getRawRecord().getId() == null) {
      record.getRawRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getParsedRecord() != null && record.getParsedRecord().getId() == null) {
      record.getParsedRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getErrorRecord() != null && record.getErrorRecord().getId() == null) {
      record.getErrorRecord().setId(UUID.randomUUID().toString());
    }
    if (record.getAdditionalInfo() == null) {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false));
    }
    return record;
  }

  @Override
  public Future<RecordBatch> saveRecords(RecordBatch recordBatch, String tenantId) {

    recordBatch.getItems().forEach(item -> {
      initFields(item.getRecord());
      preProcessItem(item, tenantId)
        .compose(it -> insertOrUpdate(it, tenantId))
        .setHandler(ar -> postProcessItem(item, ar));
    });

    return Future.succeededFuture(recordBatch);
  }

  private Future<Boolean> insertOrUpdate(Item item, String tenantId) {
    Future<Boolean> future = Future.future();
    if (StringUtils.isNotBlank(item.getErrorMessage())) {
      future = saveRecord(item.getRecord(), tenantId);
    } else {
      //todo log
      future.fail(item.getErrorMessage());
    }
    return future;
  }

  private Future<Item> preProcessItem(Item item, String tenantId) {
    return checkAssociationWithSnapshot(item.getRecord(), tenantId)
      .compose(record -> fillGeneration(record, tenantId))
      .map(item::withRecord)
      .otherwise(ex -> item.withErrorMessage(ex.getMessage()));
  }

  private void postProcessItem(Item item, AsyncResult<Boolean> ar) {
    if (ar.succeeded()) {
      item.setStatus(200);
      item.setHref("/source-storage/batch/records/" + item.getRecord().getId());
    } else {
      //todo log
      item.setStatus(422);
      item.setErrorMessage(ar.cause().getMessage());
    }
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
          format("Record with id '%s' was not found", record.getId()))))
      );
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(String query, int offset, int limit, boolean deletedRecords, String tenantId) {
    return recordDao.getSourceRecords(query, offset, limit, deletedRecords, tenantId);
  }

  @Override
  public Future<Boolean> updateParsedRecords(ParsedRecordCollection parsedRecordCollection, String tenantId) {
    if (parsedRecordCollection.getParsedRecords().stream().anyMatch(parsedRecord -> parsedRecord.getId() == null)) {
      return Future.failedFuture(new BadRequestException("Each parsed record should contain an id"));
    } else {
      ArrayList<Future> updateFutures = new ArrayList<>();
      parsedRecordCollection.getParsedRecords().forEach(parsedRecord ->
        updateFutures.add(recordDao.updateParsedRecord(parsedRecord, parsedRecordCollection.getRecordType(), tenantId)));
      return CompositeFuture.all(updateFutures).map(Future::succeeded);
    }
  }

  @Override
  public Future<Record> getFormattedRecord(SourceStorageFormattedRecordsIdGetIdentifier identifier, String id, String tenantId) {
    Future<Optional<Record>> future;
    if (identifier == SourceStorageFormattedRecordsIdGetIdentifier.INSTANCE) {
      future = recordDao.getRecordByInstanceId(id, tenantId);
    } else {
      future = getRecordById(id, tenantId);
    }
    return future.map(optionalRecord -> formatMarcRecord(optionalRecord.orElseThrow(() -> new NotFoundException(
      format("Couldn't find Record with %s id %s", identifier, id)))));
  }

  private Record formatMarcRecord(Record record) {
    try {
      String parsedRecordContent = JsonObject.mapFrom(record.getParsedRecord().getContent()).toString();
      MarcReader reader = new MarcJsonReader(new ByteArrayInputStream(parsedRecordContent.getBytes(StandardCharsets.UTF_8)));
      if (reader.hasNext()) {
        org.marc4j.marc.impl.RecordImpl marcRecord = (org.marc4j.marc.impl.RecordImpl) reader.next();
        record.setParsedRecord(record.getParsedRecord().withFormattedContent(marcRecord.toString()));
      }
    } catch (Exception e) {
      LOG.error("Couldn't format MARC record", e);
    }
    return record;
  }

}
