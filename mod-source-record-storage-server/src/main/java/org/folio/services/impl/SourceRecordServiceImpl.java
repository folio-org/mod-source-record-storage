package org.folio.services.impl;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.SourceRecordContent;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecord.RecordType;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.services.SourceRecordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

@Service
public class SourceRecordServiceImpl implements SourceRecordService {

  @Autowired
  private SourceRecordDao sourceRecordDao;

  @Autowired
  private LBRecordDao recordDao;

  @Autowired
  private RawRecordDao rawRecordDao;

  @Autowired
  private ParsedRecordDao parsedRecordDao;

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordById(String id, String tenantId) {
    return sourceRecordDao.getSourceMarcRecordById(id, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByIdAlt(String id, String tenantId) {
    return sourceRecordDao.getSourceMarcRecordByIdAlt(id, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(String instanceId,
      String tenantId) {
    return sourceRecordDao.getSourceMarcRecordByInstanceId(instanceId, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceIdAlt(String instanceId,
      String tenantId) {
    return sourceRecordDao.getSourceMarcRecordByInstanceIdAlt(instanceId, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecords(Integer offset, Integer limit,
      String tenantId) {
    return sourceRecordDao.getSourceMarcRecords(offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsAlt(Integer offset, Integer limit,
      String tenantId) {
    return sourceRecordDao.getSourceMarcRecordsAlt(offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriod(Date from, Date till,
      Integer offset, Integer limit, String tenantId) {
    return sourceRecordDao.getSourceMarcRecordsForPeriod(from, till, offset, limit, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsForPeriodAlt(Date from, Date till,
      Integer offset, Integer limit, String tenantId) {
    return sourceRecordDao.getSourceMarcRecordsForPeriodAlt(from, till, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordById(SourceRecordContent content,
      String id, String tenantId) {
    return recordDao.getById(id, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByMatchedId(SourceRecordContent content,
      String matchedId, String tenantId) {
    return recordDao.getByMatchedId(matchedId, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceMarcRecordByInstanceId(SourceRecordContent content,
      String instanceId, String tenantId) {
    return recordDao.getByInstanceId(instanceId, tenantId)
      .map(this::toSourceRecord)
      .compose(sourceRecord -> lookupContent(content, tenantId, sourceRecord));
  }

  @Override
  public Future<SourceRecordCollection> getSourceMarcRecordsByQuery(SourceRecordContent content,
      RecordQuery query, Integer offset, Integer limit, String tenantId) {
    return recordDao.getByQuery(query, offset, limit, tenantId)
      .compose(recordCollection -> lookupContent(content, tenantId, recordCollection));
  }

  @Override
  public void getSourceMarcRecordsByQuery(SourceRecordContent content, RecordQuery query,
      Integer offset, Integer limit, String tenantId, Handler<SourceRecord> handler,
      Handler<AsyncResult<Void>> endHandler) {
    sourceRecordDao.getSourceMarcRecordsByQuery(content, query, offset, limit, tenantId,
      stream -> stream
        .handler(row -> {
          stream.pause();
          lookupContent(content, tenantId, sourceRecordDao.toSourceRecord(row)).onComplete(res -> {
            if (res.failed()) {
              endHandler.handle(Future.failedFuture(res.cause()));
              return;
            }
            handler.handle(res.result());
            stream.resume();
          });
        }).exceptionHandler(e -> endHandler.handle(Future.failedFuture(e))),
      endHandler);
  }

  @Override
  public SourceRecord toSourceRecord(Record record) {
    return new SourceRecord()
      .withRecordId(record.getId())
      .withSnapshotId(record.getSnapshotId())
      .withOrder(record.getOrder())
      // NOTE: not ideal to have multiple record type enums
      .withRecordType(RecordType.fromValue(record.getRecordType().toString()))
      .withMetadata(record.getMetadata());
  }

  private Optional<SourceRecord> toSourceRecord(Optional<Record> record) {
    if (record.isPresent()) {
      return Optional.of(toSourceRecord(record.get()));
    }
    return Optional.empty();
  }

  private Future<SourceRecordCollection> lookupContent(SourceRecordContent content, String tenantId,
      RecordCollection recordCollection) {
    Promise<SourceRecordCollection> promise = Promise.promise();
    CompositeFuture.all(
      recordCollection.getRecords().stream()
        .map(this::toSourceRecord)
        .map(sr -> lookupContent(content, tenantId, sr))
        .collect(Collectors.toList())
    ).onComplete(lookup -> {
      List<SourceRecord> sourceRecords = lookup.result().list();
      promise.complete(new SourceRecordCollection()
        .withSourceRecords(sourceRecords)
        .withTotalRecords(recordCollection.getTotalRecords()));
    });
    return promise.future();
  }

  private Future<Optional<SourceRecord>> lookupContent(SourceRecordContent content, String tenantId,
      Optional<SourceRecord> sourceRecord) {
    if (sourceRecord.isPresent()) {
      return lookupContent(content, tenantId, sourceRecord.get()).map(Optional::of);
    }
    return Future.factory.succeededFuture(sourceRecord);
  }

  private Future<SourceRecord> lookupContent(SourceRecordContent content, String tenantId,
      SourceRecord sourceRecord) {
    String id = sourceRecord.getRecordId();
    Promise<SourceRecord> promise = Promise.promise();
    switch(content) {
      case RAW_AND_PARSED_RECORD:
        CompositeFuture.all(
          rawRecordDao.getById(id, tenantId).map(rawRecord -> addRawRecordContent(sourceRecord, rawRecord)),
          parsedRecordDao.getById(id, tenantId).map(parsedRecord -> addParsedRecordContent(sourceRecord, parsedRecord))
        ).onComplete(lookup -> promise.complete(sourceRecord));
        break;
      case PARSED_RECORD_ONLY:
        parsedRecordDao.getById(id, tenantId).map(parsedRecord -> addParsedRecordContent(sourceRecord, parsedRecord))
          .onComplete(lookup -> promise.complete(sourceRecord));
        break;
      case RAW_RECORD_ONLY:
        rawRecordDao.getById(id, tenantId).map(rawRecord -> addRawRecordContent(sourceRecord, rawRecord))
          .onComplete(lookup -> promise.complete(sourceRecord));
        break;
    }
    return promise.future();
  }

  private SourceRecord addRawRecordContent(SourceRecord sourceRecord, Optional<RawRecord> rawRecord) {
    if (rawRecord.isPresent()) {
      sourceRecord.withRawRecord(rawRecord.get());
    }
    return sourceRecord;
  }

  private SourceRecord addParsedRecordContent(SourceRecord sourceRecord, Optional<ParsedRecord> parsedRecord) {
    if (parsedRecord.isPresent()) {
      sourceRecord.withParsedRecord(parsedRecord.get());
    }
    return sourceRecord;
  }

}