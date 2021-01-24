package org.folio.services;

import static java.lang.String.format;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordForeignKeys;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordHasId;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordHasSuppressDiscovery;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.logging.log4j.util.Strings;
import org.folio.dao.RecordDao;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.reactivex.Flowable;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
public class RecordServiceImpl implements RecordService {

  private static final Logger LOG = LoggerFactory.getLogger(RecordServiceImpl.class);

  private final RecordDao recordDao;

  @Autowired
  public RecordServiceImpl(final RecordDao recordDao) {
    this.recordDao = recordDao;
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset,
      int limit, String tenantId) {
    return recordDao.getRecords(condition, recordType, orderFields, offset, limit, tenantId);
  }

  @Override
  public Flowable<Record> streamRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    return recordDao.streamRecords(condition, recordType, orderFields, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String id, String tenantId) {
    return recordDao.getRecordById(id, tenantId);
  }

  @Override
  public Future<Record> saveRecord(Record record, String tenantId) {
    ensureRecordHasId(record);
    ensureRecordHasSuppressDiscovery(record);
    return recordDao.executeInTransaction(txQE -> SnapshotDaoUtil.findById(txQE, record.getSnapshotId())
      .map(optionalSnapshot -> optionalSnapshot
        .orElseThrow(() -> new NotFoundException(format(SNAPSHOT_NOT_FOUND_TEMPLATE, record.getSnapshotId()))))
      .compose(snapshot -> {
        if (Objects.isNull(snapshot.getProcessingStartedDate())) {
          return Future.failedFuture(new BadRequestException(format(SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE, snapshot.getStatus())));
        }
        return Future.succeededFuture();
      })
      .compose(v -> {
        if (Objects.isNull(record.getGeneration())) {
          return recordDao.calculateGeneration(txQE, record);
        }
        return Future.succeededFuture(record.getGeneration());
      })
      .compose(generation -> {
        if (generation > 0) {
          return recordDao.getRecordByMatchedId(txQE, record.getMatchedId())
            .compose(optionalMatchedRecord -> optionalMatchedRecord
              .map(matchedRecord -> recordDao.saveUpdatedRecord(txQE, ensureRecordForeignKeys(record.withGeneration(generation)), matchedRecord.withState(Record.State.OLD)))
              .orElse(recordDao.saveRecord(txQE, ensureRecordForeignKeys(record.withGeneration(generation)))));
        } else {
          return recordDao.saveRecord(txQE, ensureRecordForeignKeys(record.withGeneration(generation)));
        }
      }), tenantId);
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId) {
    if (recordCollection.getRecords().isEmpty()) {
      Promise<RecordsBatchResponse> promise = Promise.promise();
      promise.complete(new RecordsBatchResponse().withTotalRecords(0));
      return promise.future();
    }
    return recordDao.saveRecords(recordCollection, tenantId);
  }

  @Override
  public Future<Record> updateRecord(Record record, String tenantId) {
    return recordDao.updateRecord(ensureRecordForeignKeys(record), tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    return recordDao.getSourceRecords(condition, recordType, orderFields, offset, limit, tenantId);
  }

  @Override
  public Flowable<SourceRecord> streamSourceRecords(Condition condition, RecordType recordType, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    return recordDao.streamSourceRecords(condition, recordType, orderFields, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(List<String> ids, ExternalIdType externalIdType, RecordType recordType, Boolean deleted, String tenantId) {
    return recordDao.getSourceRecords(ids, externalIdType, recordType, deleted, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, ExternalIdType externalIdType, String tenantId) {
    return recordDao.getSourceRecordByExternalId(id, externalIdType, tenantId);
  }

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = recordCollection.getRecords().stream()
      .map(this::validateParsedRecordId)
      .map(record -> recordDao.updateParsedRecord(record, tenantId))
      .collect(Collectors.toList());
    Promise<ParsedRecordsBatchResponse> promise = Promise.promise();
    CompositeFuture.join(futures).onComplete(ar -> {
      ParsedRecordsBatchResponse response = new ParsedRecordsBatchResponse();
      futures.forEach(update -> {
        if (update.failed()) {
          response.getErrorMessages().add(update.cause().getMessage());
        } else {
          response.getParsedRecords().add((ParsedRecord) update.result());
        }
      });
      response.setTotalRecords(response.getParsedRecords().size());
      promise.complete(response);
    });
    return promise.future();
  }

  @Override
  public Future<Record> getFormattedRecord(String id, ExternalIdType externalIdType, String tenantId) {
    return recordDao.getRecordByExternalId(id, externalIdType, tenantId)
      .map(optionalRecord -> formatMarcRecord(optionalRecord.orElseThrow(() ->
        new NotFoundException(format("Couldn't find record with id type %s and id %s", externalIdType, id)))));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, ExternalIdType externalIdType, Boolean suppress, String tenantId) {
    return recordDao.updateSuppressFromDiscoveryForRecord(id, externalIdType, suppress, tenantId);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return recordDao.deleteRecordsBySnapshotId(snapshotId, tenantId);
  }

  @Override
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId) {
    String newRecordId = UUID.randomUUID().toString();
    return recordDao.executeInTransaction(txQE -> recordDao.getRecordByMatchedId(txQE, parsedRecordDto.getId())
      .compose(optionalRecord -> optionalRecord
        .map(existingRecord -> SnapshotDaoUtil.save(txQE, new Snapshot()
          .withJobExecutionId(snapshotId)
          .withStatus(Snapshot.Status.COMMITTED)) // no processing of the record is performed apart from the update itself
            .compose(snapshot -> recordDao.saveUpdatedRecord(txQE, new Record()
              .withId(newRecordId)
              .withSnapshotId(snapshot.getJobExecutionId())
              .withMatchedId(parsedRecordDto.getId())
              .withRecordType(Record.RecordType.fromValue(parsedRecordDto.getRecordType().value()))
              .withState(Record.State.ACTUAL)
              .withOrder(existingRecord.getOrder())
              .withGeneration(existingRecord.getGeneration() + 1)
              .withRawRecord(new RawRecord().withId(newRecordId).withContent(existingRecord.getRawRecord().getContent()))
              .withParsedRecord(new ParsedRecord().withId(newRecordId).withContent(parsedRecordDto.getParsedRecord().getContent()))
              .withExternalIdsHolder(parsedRecordDto.getExternalIdsHolder())
              .withAdditionalInfo(parsedRecordDto.getAdditionalInfo())
              .withMetadata(parsedRecordDto.getMetadata()), existingRecord.withState(Record.State.OLD))))
        .orElse(Future.failedFuture(new NotFoundException(
          format("Record with id '%s' was not found", parsedRecordDto.getId()))))), tenantId);
  }

  private Record validateParsedRecordId(Record record) {
    if (Objects.isNull(record.getParsedRecord()) || Strings.isEmpty(record.getParsedRecord().getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
    return record;
  }

  private Record formatMarcRecord(Record record) {
    try {
      RecordDaoUtil.formatRecord(record);
    } catch (Exception e) {
      LOG.error("Couldn't format {} record", e, record.getRecordType());
    }
    return record;
  }

}
