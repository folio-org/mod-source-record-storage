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
import static org.folio.services.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.util.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.util.AdditionalFieldsUtil.getFieldFromMarcRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.services.exceptions.DuplicateRecordException;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.folio.dao.RecordDao;
import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.FetchParsedRecordsBatchRequest;
import org.folio.rest.jaxrs.model.FieldRange;
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
import org.folio.rest.jaxrs.model.StrippedParsedRecordCollection;
import org.folio.rest.jooq.enums.RecordState;
import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.folio.services.util.parser.SearchExpressionParser;

@Service
public class RecordServiceImpl implements RecordService {

  private static final Logger LOG = LogManager.getLogger();

  private final RecordDao recordDao;
  private static final String DUPLICATE_CONSTRAINT = "idx_records_matched_id_gen";
  private static final String DUPLICATE_RECORD_MSG = "Incoming file may contain duplicates";
  public static final char SUBFIELD_S = 's';
  public static final char INDICATOR = 'f';

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
    LOG.debug(format("saveRecord:: Saving record with id: %s for tenant: %s", record.getId(), tenantId));
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
        .compose(v -> setMatchedIdForRecord(record, tenantId))
        .compose(r -> {
          if (Objects.isNull(r.getGeneration())) {
            return recordDao.calculateGeneration(txQE, r);
          }
          return Future.succeededFuture(r.getGeneration());
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
        }), tenantId)
      .recover(RecordServiceImpl::mapToDuplicateExceptionIfNeeded);
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId) {
    if (recordCollection.getRecords().isEmpty()) {
      Promise<RecordsBatchResponse> promise = Promise.promise();
      promise.complete(new RecordsBatchResponse().withTotalRecords(0));
      return promise.future();
    }
    List<Future> setMatchedIdsFutures = new ArrayList<>();
    recordCollection.getRecords().forEach(record -> setMatchedIdsFutures.add(setMatchedIdForRecord(record, tenantId)));
    return GenericCompositeFuture.all(setMatchedIdsFutures)
      .compose(ar -> ar.succeeded() ?
        recordDao.saveRecords(recordCollection, tenantId)
        : Future.failedFuture(ar.cause()))
      .recover(RecordServiceImpl::mapToDuplicateExceptionIfNeeded);
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
  public Future<StrippedParsedRecordCollection> fetchStrippedParsedRecords(FetchParsedRecordsBatchRequest fetchRequest, String tenantId) {
    var ids = fetchRequest.getConditions().getIds();
    var idType = IdType.valueOf(fetchRequest.getConditions().getIdType());
    if (ids.isEmpty()) {
      Promise<StrippedParsedRecordCollection> promise = Promise.promise();
      promise.complete(new StrippedParsedRecordCollection().withTotalRecords(0));
      return promise.future();
    }

    var recordType = toRecordType(fetchRequest.getRecordType().name());
    return recordDao.getStrippedParsedRecords(ids, idType, recordType, tenantId)
      .onComplete(records -> filterFieldsByDataRange(records, fetchRequest))
      .onFailure(ex -> {
        LOG.warn("fetchParsedRecords:: Failed to fetch parsed records. {}", ex.getMessage());
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
  public Future<Void> deleteRecordsByExternalId(String externalId, String tenantId) {
    return recordDao.deleteRecordsByExternalId(externalId, tenantId).map(b -> null);
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
  public Future<Void> updateRecordsState(String matchedId, RecordState state, RecordType recordType, String tenantId) {
    return recordDao.updateRecordsState(matchedId, state, recordType, tenantId);
  }

  private Future<Record> setMatchedIdForRecord(Record record, String tenantId) {
    String marcField999s = getFieldFromMarcRecord(record, TAG_999, INDICATOR, INDICATOR, SUBFIELD_S);
    if (marcField999s != null) {
      // Set matched id from 999$s marc field
      LOG.debug(format("setMatchedIdForRecord:: Set matchedId: %s from 999$s field for record with id: %s", marcField999s, record.getId()));
      return Future.succeededFuture(record.withMatchedId(marcField999s));
    }
    Promise<Record> promise = Promise.promise();
    String externalId = RecordDaoUtil.getExternalId(record.getExternalIdsHolder(), record.getRecordType());
    IdType idType = RecordDaoUtil.getExternalIdType(record.getRecordType());

    if (externalId != null && idType != null && record.getState() == Record.State.ACTUAL) {
      setMatchedIdFromExistingSourceRecord(record, tenantId, promise, externalId, idType);
    } else {
      // Set matched id same as record id
      promise.complete(record.withMatchedId(record.getId()));
    }

    return promise.future().onSuccess(r -> {
      if (record.getRecordType() != null && !record.getRecordType().equals(Record.RecordType.EDIFACT)) {
        addFieldToMarcRecord(r, TAG_999, SUBFIELD_S, r.getMatchedId());
      }
    });
  }

  private void setMatchedIdFromExistingSourceRecord(Record record, String tenantId, Promise<Record> promise, String externalId, IdType idType) {
    recordDao.getSourceRecordByExternalId(externalId, idType, RecordState.ACTUAL, tenantId)
      .onComplete((ar) -> {
        if (ar.succeeded()) {
          Optional<SourceRecord> sourceRecord = ar.result();
          if (sourceRecord.isPresent()) {
            // Set matched id from existing source record
            String sourceRecordId = sourceRecord.get().getRecordId();
            LOG.debug(format("setMatchedIdFromExistingSourceRecord:: Set matchedId: %s from source record for record with id: %s",
              sourceRecordId, record.getId()));
            promise.complete(record.withMatchedId(sourceRecordId));
          } else {
            // Set matched id same as record id
            LOG.debug(format("setMatchedIdFromExistingSourceRecord:: Set matchedId same as record id: %s", record.getId()));
            promise.complete(record.withMatchedId(record.getId()));
          }
        } else {
          LOG.warn("setMatchedIdFromExistingSourceRecord:: Error while retrieving source record");
          promise.fail(ar.cause());
        }
      });
  }

  private static Future mapToDuplicateExceptionIfNeeded(Throwable throwable) {
    if (throwable instanceof PgException pgException) {
      if (StringUtils.equals(pgException.getConstraint(), DUPLICATE_CONSTRAINT)) {
        return Future.failedFuture(new DuplicateRecordException(DUPLICATE_RECORD_MSG));
      }
    }
    return Future.failedFuture(throwable);
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

  private void filterFieldsByDataRange(AsyncResult<StrippedParsedRecordCollection> recordCollectionAsyncResult,
                                       FetchParsedRecordsBatchRequest fetchRequest) {
    var data = fetchRequest.getData();
    if (data != null && !data.isEmpty()) {
      recordCollectionAsyncResult.result().getRecords().stream()
        .filter(recordToFilter -> nonNull(recordToFilter.getParsedRecord()))
        .forEach(recordToFilter -> {
          JsonObject parsedContent = JsonObject.mapFrom(recordToFilter.getParsedRecord().getContent());
          JsonArray fields = parsedContent.getJsonArray("fields");

          var filteredFields = fields.stream().parallel()
            .map(JsonObject.class::cast)
            .filter(field -> checkFieldRange(field, data))
            .map(JsonObject::getMap)
            .collect(Collectors.toList());

          parsedContent.put("fields", filteredFields);
          recordToFilter.getParsedRecord().setContent(parsedContent.getMap());
        });
    }
  }

  private boolean checkFieldRange(JsonObject fields, List<FieldRange> data) {
    var field = fields.fieldNames().iterator().next();
    int intField = Integer.parseInt(field);

    for (var range : data) {
      if (intField >= Integer.parseInt(range.getFrom()) &&
        intField <= Integer.parseInt(range.getTo())) {
        return true;
      }
    }
    return false;
  }
}
