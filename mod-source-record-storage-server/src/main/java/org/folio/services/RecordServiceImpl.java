package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.RecordDao;
import org.folio.dao.SnapshotDao;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SourceStorageFormattedRecordsIdGetIdentifier;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Component
public class RecordServiceImpl implements RecordService {

  private static final Logger LOG = LoggerFactory.getLogger(RecordServiceImpl.class);

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
  public Future<Record> saveRecord(Record record, String tenantId) {
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
    return snapshotDao.getSnapshotById(record.getSnapshotId(), tenantId)
      .map(optionalRecordSnapshot -> optionalRecordSnapshot.orElseThrow(() -> new NotFoundException("Couldn't find snapshot with id " + record.getSnapshotId())))
      .compose(snapshot -> {
        if (snapshot.getProcessingStartedDate() == null) {
          return Future.failedFuture(new BadRequestException(
            format("Date when processing started is not set, expected snapshot status is PARSING_IN_PROGRESS, actual - %s", snapshot.getStatus())));
        }
        return Future.succeededFuture();
      })
      .compose(f -> recordDao.calculateGeneration(record, tenantId))
      .compose(generation -> recordDao.saveRecord(record.withGeneration(generation), tenantId));
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId) {

    List<Future> futures = recordCollection.getRecords().stream()
      .map(record -> saveRecord(record, tenantId))
      .collect(Collectors.toList());

    Future<RecordsBatchResponse> result = Future.future();

    CompositeFuture.join(futures).setHandler(ar -> {
        RecordsBatchResponse response = new RecordsBatchResponse();
        futures.forEach(save -> {
          if (save.failed()) {
            response.getErrorMessages().add(save.cause().getMessage());
          } else {
            response.getRecords().add((Record) save.result());
          }
        });
        response.setTotalRecords(response.getRecords().size());
        result.complete(response);
      }
    );
    return result;
  }

  @Override
  public Future<Record> updateRecord(Record record, String tenantId) {
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
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(ParsedRecordCollection parsedRecordCollection, String tenantId) {

    List<Future> futures = parsedRecordCollection.getParsedRecords().stream()
      .peek(this::validateParsedRecordId)
      .map(parsedRecord -> recordDao.updateParsedRecord(parsedRecord, parsedRecordCollection.getRecordType(), tenantId))
      .collect(Collectors.toList());

    Future<ParsedRecordsBatchResponse> result = Future.future();

    CompositeFuture.join(futures).setHandler(ar -> {
        ParsedRecordsBatchResponse response = new ParsedRecordsBatchResponse();
        futures.forEach(update -> {
          if (update.failed()) {
            response.getErrorMessages().add(update.cause().getMessage());
          } else {
            response.getParsedRecords().add((ParsedRecord) update.result());
          }
        });
        response.setTotalRecords(response.getParsedRecords().size());
        result.complete(response);
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

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId) {
    return recordDao.updateSuppressFromDiscoveryForRecord(suppressFromDiscoveryDto, tenantId);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return recordDao.deleteRecordsBySnapshotId(snapshotId, tenantId);
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
