package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

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

  @Override
  public Future<RecordBatch> saveRecords(RecordBatch recordBatch, String tenantId) {
    List<Future> items = recordBatch.getItems().stream()
      .map(it -> it.withRecord(initFields(it.getRecord())))
      .map(it -> checkAssociationWithSnapshot(it, tenantId))
      .map(future -> future.compose(it -> insertOrUpdate(it, tenantId)))
      .collect(Collectors.toList());

    return CompositeFuture.all(items).compose(future -> Future.succeededFuture(recordBatch.withItems(future.list())));
  }

  private Future<Item> checkAssociationWithSnapshot(Item item, String tenantId) {
    return checkAssociationWithSnapshot(item.getRecord(), tenantId)
      .compose(record -> fillGeneration(record, tenantId))
      .map(item::withRecord)
      .otherwise(ex -> handleException(item, ex));
  }

  private Future<Record> checkAssociationWithSnapshot(Record record, String tenantId) {
    return snapshotDao.getSnapshotById(record.getSnapshotId(), tenantId)
      .map(optional-> optional.orElse(null))
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

  private Future<Item> insertOrUpdate(Item item, String tenantId) {
    Future<Item> future = Future.future();
    if (isBlank(item.getErrorMessage())) {
      saveRecord(item.getRecord(), tenantId)
        .map(handleSuccess(item))
        .otherwise(ex -> handleException(item, ex))
        .setHandler(future.completer());
    } else {
      LOG.error("An error has occurred during saving a record: " + item.getErrorMessage());
      future.complete(item);
    }
    return future;
  }

  private Item handleException(Item item, Throwable ex) {
    if (ex instanceof NotFoundException) {
      item.withStatus(404);
    } else if (ex instanceof BadRequestException) {
      item.withStatus(400);
    } else {
      item.withStatus(500);
    }
    return item.withErrorMessage(ex.getMessage());
  }

  private Item handleSuccess(Item item) {
    return item.withStatus(200).withHref("/source-storage/batch/records/" + item.getRecord().getId());
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
  public Future<ParsedRecordCollection> updateParsedRecords(ParsedRecordCollection parsedRecordCollection, String tenantId) {

    Map<ParsedRecord, Future<Boolean>> updatedRecords = parsedRecordCollection.getParsedRecords().stream()
      .peek(this::validateParsedRecordId)
      .map(it -> Pair.of(it, recordDao.updateParsedRecord(it, parsedRecordCollection.getRecordType(), tenantId)))
      .collect(LinkedHashMap::new, (map, pair) -> map.put(pair.getKey(), pair.getValue()), Map::putAll);

    Future<ParsedRecordCollection> result = Future.future();

    CompositeFuture.join(new ArrayList<>(updatedRecords.values())).setHandler(ar -> {
        ParsedRecordCollection records = new ParsedRecordCollection();
        updatedRecords.forEach((record, future) -> {
          if (future.failed()) {
            records.getErrorMessages().add(future.cause().getMessage());
          } else {
            records.getParsedRecords().add(record);
          }
        });
        records.setTotalRecords(records.getParsedRecords().size());
        result.complete(records);
      }
    );
    return result;
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

  private void validateParsedRecordId(ParsedRecord record) {
    if (Objects.isNull(record.getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
  }

}
