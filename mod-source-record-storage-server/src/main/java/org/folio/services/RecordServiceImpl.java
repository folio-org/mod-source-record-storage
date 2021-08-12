package org.folio.services;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import static org.folio.dao.util.RecordDaoUtil.RECORD_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordForeignKeys;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordHasId;
import static org.folio.dao.util.RecordDaoUtil.ensureRecordHasSuppressDiscovery;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_FOUND_TEMPLATE;
import static org.folio.dao.util.SnapshotDaoUtil.SNAPSHOT_NOT_STARTED_MESSAGE_TEMPLATE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.util.QueryParamUtil.toRecordType;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import io.reactivex.Flowable;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.sqlclient.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.ControlField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import org.folio.dao.RecordDao;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.RecordType;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
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
import org.folio.services.util.EventHandlingUtil;
import org.folio.services.util.parser.ParseFieldsResult;
import org.folio.services.util.parser.ParseLeaderResult;
import org.folio.services.util.parser.SearchExpressionParser;

@Service
public class RecordServiceImpl implements RecordService {

  private static final Logger LOG = LogManager.getLogger();
  private static final String TAG_004 = "004";
  public static final String ERROR_KEY = "ERROR";
  private static final AtomicInteger chunkCounter = new AtomicInteger();
  private static final AtomicInteger indexer = new AtomicInteger();
  private KafkaConfig kafkaConfig;
  @Value("${srs.kafka.ParsedRecordChunksKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

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
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String id, List<KafkaHeader> kafkaHeaders, String jobExecutionId) {
    if (recordCollection.getRecords().isEmpty()) {
      Promise<RecordsBatchResponse> promise = Promise.promise();
      promise.complete(new RecordsBatchResponse().withTotalRecords(0));
      return promise.future();
    }
    recordCollection = filterMarcHoldingsBy004Field(recordCollection, jobExecutionId, kafkaHeaders, jobExecutionId);
    return recordDao.saveRecords(recordCollection, jobExecutionId);
  }

  private RecordCollection filterMarcHoldingsBy004Field(RecordCollection recordCollection, String tenantId, List<KafkaHeader> kafkaHeaders, String jobExecutionId) {
    var records = recordCollection.getRecords();
    var marcHoldingsWithoutMarcBib = records.stream()
      .filter(record -> record.getRecordType() == Record.RecordType.MARC_HOLDING)
      .filter(record -> isNotBlank(getControlFieldValue(record, TAG_004)))
      .filter(record -> {
        JsonArray jsonArray = findMarcBibBy004Field(tenantId, record);
        LOG.info("The MARC Holdings record with id = {} and MARC Bib = {}", record.getId(), jsonArray);
        return !jsonArray.isEmpty();
        }
      ).collect(Collectors.toList());

    if(!marcHoldingsWithoutMarcBib.isEmpty()){
      var marcHoldings = new RecordCollection().withRecords(marcHoldingsWithoutMarcBib).withTotalRecords(marcHoldingsWithoutMarcBib.size());
      LOG.info("The MARC Holdings record collections doesn't save, count of elements: {}", marcHoldings.getTotalRecords());
      sendErrorRecordsSavingEvents(marcHoldings, "The MARC Holdings record has not MARC Bib", kafkaHeaders, jobExecutionId, tenantId)
        .compose(v -> Future.failedFuture("The MARC Holdings record has not MARC Bib"));
    }

    records.removeAll(marcHoldingsWithoutMarcBib);
    return new RecordCollection().withRecords(records).withTotalRecords(records.size());
  }

  private Future<Void> sendErrorRecordsSavingEvents(RecordCollection recordCollection, String message, List<KafkaHeader> kafkaHeaders, String jobExecutionId, String tenantId) {
    List<Future<Boolean>> sendingFutures = new ArrayList<>();
    for (Record record : recordCollection.getRecords()) {
      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(DI_ERROR.value())
        .withJobExecutionId(jobExecutionId)
        .withEventsChain(List.of(DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value()))
        .withTenant(tenantId)
        .withContext(new HashMap<>(){{
          put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
          put(ERROR_KEY, message);
        }});

      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
      sendingFutures.add(EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(dataImportEventPayload), DI_ERROR.value(), kafkaHeaders, kafkaConfig, key));
    }

    Promise<Void> promise = Promise.promise();
    GenericCompositeFuture.join(sendingFutures)
      .onSuccess(v -> promise.complete())
      .onFailure(th -> {
        LOG.warn("Failed to send records sending error events" , th);
        promise.fail(th);
      });

    return promise.future();
  }

  private JsonArray findMarcBibBy004Field(String tenantId, Record record) {
    var controlFieldValue = getControlFieldValue(record, TAG_004);
    var recordSearchParameters = RecordSearchParameters.from(getMarcRecordSearchRequest(controlFieldValue));
    LOG.info("Prepare record search parameter: {} ", recordSearchParameters);
    var rowFlowable = streamMarcRecordIds(recordSearchParameters, tenantId);
    var jsonObjectFlowable = rowFlowable.map(Row::toJson);
    return jsonObjectFlowable.firstElement().blockingGet().getJsonArray("records");
  }

  public static String getControlFieldValue(Record record, String tag) {
    if (record != null && record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      MarcReader reader = buildMarcReader(record);
      try {
        if (reader.hasNext()) {
          org.marc4j.marc.Record marcRecord = reader.next();
          return marcRecord.getControlFields().stream()
            .filter(controlField -> controlField.getTag().equals(tag))
            .findFirst()
            .map(ControlField::getData)
            .orElse(null);
        }
      } catch (Exception e) {
        LOG.error("Error during the search a field in the record", e);
        return null;
      }
    }
    return null;
  }

  private MarcRecordSearchRequest getMarcRecordSearchRequest(String controlFieldValue) {
    MarcRecordSearchRequest marcRecordSearchRequest = new MarcRecordSearchRequest();
    marcRecordSearchRequest.setFieldsSearchExpression("001.value = '" + controlFieldValue + "'");
    return marcRecordSearchRequest;
  }

  private static MarcReader buildMarcReader(Record record) {
    return new MarcJsonReader(new ByteArrayInputStream(record.getParsedRecord().getContent().toString().getBytes(StandardCharsets.UTF_8)));
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
  public Future<SourceRecordCollection> getSourceRecords(List<String> ids, ExternalIdType externalIdType, RecordType recordType, Boolean deleted, String tenantId) {
    return recordDao.getSourceRecords(ids, externalIdType, recordType, deleted, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, ExternalIdType externalIdType, String tenantId) {
    return recordDao.getSourceRecordByExternalId(id, externalIdType, tenantId);
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

  private Record formatMarcRecord(Record record) {
    try {
      RecordType recordType = toRecordType(record.getRecordType().name());
      recordType.formatRecord(record);
    } catch (Exception e) {
      LOG.error("Couldn't format {} record", record.getRecordType(), e );
    }
    return record;
  }

}
