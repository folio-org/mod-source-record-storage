package org.folio.services;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.dao.util.RecordDaoUtil.RECORD_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordForeignKeys;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordHasId;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordHasSuppressDiscovery;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE;
import static org.folio.rest.util.QueryParamUtil.toRecordType;

import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.RecordDao;
import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.FetchParsedRecordsBatchRequest;
import org.folio.rest.jaxrs.model.MarcBibCollection;
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
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.folio.services.util.parser.SearchExpressionParser;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RecordServiceImpl implements RecordService {

  private static final Logger LOG = LogManager.getLogger();

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
              .orElseGet(() -> recordDao.saveRecord(txQE, ensureRecordForeignKeys(record.withGeneration(generation)))));
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
  public Flowable<Row> streamMarcRecordIds(RecordSearchParameters searchParameters, String tenantId) {
    if (searchParameters.getLeaderSearchExpression() == null && searchParameters.getFieldsSearchExpression() == null) {
      throw new IllegalArgumentException("The 'leaderSearchExpression' and the 'fieldsSearchExpression' are missing");
    }
    ParseLeaderResult parseLeaderResult = SearchExpressionParser.parseLeaderSearchExpression(searchParameters.getLeaderSearchExpression());
    ParseFieldsResult parseFieldsResult = SearchExpressionParser.parseFieldsSearchExpression(searchParameters.getFieldsSearchExpression());
    return recordDao.streamMarcRecordIds(parseLeaderResult, parseFieldsResult, searchParameters, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(List<String> ids, IdType idType, RecordType recordType, Boolean deleted, String tenantId) {
    return recordDao.getSourceRecords(ids, idType, recordType, deleted, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, IdType idType, RecordState state, String tenantId) {
    return recordDao.getSourceRecordByExternalId(id, idType, state, tenantId);
  }

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    return recordDao.updateParsedRecord(record, tenantId);
  }

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId) {
    if (recordCollection.getRecords().isEmpty()) {
      Promise<ParsedRecordsBatchResponse> promise = Promise.promise();
      promise.complete(new ParsedRecordsBatchResponse().withTotalRecords(0));
      return promise.future();
    }
    return recordDao.updateParsedRecords(recordCollection, tenantId);
  }

  @Override
  public Future<SourceRecordCollection> fetchParsedRecords(FetchParsedRecordsBatchRequest fetchRequest, String tenantId) {
    var ids = fetchRequest.getConditions().getIds();
    var idType = IdType.valueOf(fetchRequest.getConditions().getIdType());
    if (ids.isEmpty()) {
      Promise<SourceRecordCollection> promise = Promise.promise();
      promise.complete(new SourceRecordCollection().withTotalRecords(0));
      return promise.future();
    }

    var recordType = toRecordType(fetchRequest.getRecordType().name());
    return recordDao.getSourceRecords(ids, idType, recordType, true, tenantId)
      .onComplete(records -> filterFieldsByDataRange(records, fetchRequest))
      .onFailure(ex -> {
        LOG.warn("fetchParsedRecords:: Failed to fetch parsed records {}", ex.getMessage());
        throw new BadRequestException(ex.getCause());
      });
  }

  @Override
  public Future<Record> getFormattedRecord(String id, IdType idType, String tenantId) {
    return recordDao.getRecordByExternalId(id, idType, tenantId)
      .map(optionalRecord -> formatMarcRecord(optionalRecord.orElseThrow(() ->
        new NotFoundException(format("Couldn't find record with id type %s and id %s", idType, id)))));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(String id, IdType idType, Boolean suppress, String tenantId) {
    return recordDao.updateSuppressFromDiscoveryForRecord(id, idType, suppress, tenantId);
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
          .withProcessingStartedDate(new Date())
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
          format(RECORD_NOT_FOUND_TEMPLATE, parsedRecordDto.getId()))))), tenantId);
  }

  @Override
  public Future<MarcBibCollection> verifyMarcBibRecords(List<String> marcBibIds, String tenantId) {
    if (marcBibIds.size() > Short.MAX_VALUE) {
      throw new BadRequestException("The number of IDs should not exceed 32767");
    }
    return recordDao.verifyMarcBibRecords(marcBibIds, tenantId);
  }

  @Override
  public Future<Void> updateRecordsState(String matchedId, RecordState state, String tenantId) {
    return recordDao.updateRecordsState(matchedId, state, tenantId);
  }

  private Record formatMarcRecord(Record record) {
    try {
      RecordType recordType = toRecordType(record.getRecordType().name());
      recordType.formatRecord(record);
    } catch (Exception e) {
      LOG.warn("formatMarcRecord:: Couldn't format {} record", record.getRecordType(), e);
    }
    return record;
  }

  private void filterFieldsByDataRange(AsyncResult<SourceRecordCollection> recordCollectionAsyncResult,
                                       FetchParsedRecordsBatchRequest fetchRequest) {
    recordCollectionAsyncResult.result().getSourceRecords().stream()
      .filter(recordToFilter -> nonNull(recordToFilter.getParsedRecord()))
      .forEach(recordToFilter -> {
        JsonObject parsedContent = JsonObject.mapFrom(recordToFilter.getParsedRecord().getContent());
        JsonArray fields = parsedContent.getJsonArray("fields");

        var filteredFields = fields.stream().parallel()
          .map(JsonObject.class::cast)
          .filter(field -> checkFieldRange(field, fetchRequest))
          .map(JsonObject::getMap)
          .collect(Collectors.toList());

        parsedContent.put("fields", filteredFields);
        recordToFilter.getParsedRecord().setContent(parsedContent.getMap());
      });
  }

  private boolean checkFieldRange(JsonObject fields, FetchParsedRecordsBatchRequest fetchRequest) {
    var field = fields.fieldNames().iterator().next();
    int intField = Integer.parseInt(field);

    for (var range : fetchRequest.getData()) {
      if (intField >= Integer.parseInt(range.getFrom()) &&
        intField <= Integer.parseInt(range.getTo())) {
        return true;
      }
    }
    return false;
  }
}
