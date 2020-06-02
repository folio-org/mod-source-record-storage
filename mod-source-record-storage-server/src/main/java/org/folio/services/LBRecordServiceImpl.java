package org.folio.services;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.util.ExternalIdType;
import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
@ConditionalOnProperty(prefix = "jooq", name = "services.record", havingValue = "true")
public class LBRecordServiceImpl implements LBRecordService {

  private static final Logger LOG = LoggerFactory.getLogger(LBRecordServiceImpl.class);

  private final LBRecordDao recordDao;

  @Autowired
  public LBRecordServiceImpl(final LBRecordDao recordDao) {
    this.recordDao = recordDao;
  }

  @Override
  public Future<RecordCollection> getRecords(Condition condition, Collection<OrderField<?>> orderFields, int offset,
      int limit, String tenantId) {
    return recordDao.getRecords(condition, orderFields, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<Record>> getRecordById(String matchedId, String tenantId) {
    return recordDao.getRecordById(matchedId, tenantId);
  }

  @Override
  public Future<Record> saveRecord(Record record, String tenantId) {
    if (Objects.isNull(record.getId())) {
      record.setId(UUID.randomUUID().toString());
    }
    if (Objects.isNull(record.getAdditionalInfo()) || Objects.isNull(record.getAdditionalInfo().getSuppressDiscovery())) {
      record.setAdditionalInfo(new AdditionalInfo().withSuppressDiscovery(false));
    }
    // NOTE: snapshot lookup/validation and generation calculation moved to DAO in order to perform transactionally
    return recordDao.saveRecord(ensureRecordForeignKeys(record), tenantId);
  }

  @Override
  public Future<RecordsBatchResponse> saveRecords(RecordCollection recordCollection, String tenantId) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = recordCollection.getRecords().stream()
      .map(record -> saveRecord(record, tenantId))
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
  public Future<Record> updateRecord(Record record, String tenantId) {
    return recordDao.updateRecord(ensureRecordForeignKeys(record), tenantId);
  }

  @Override
  public Future<SourceRecordCollection> getSourceRecords(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    // NOTE: new schema did not have deleted property
    return recordDao.getSourceRecords(condition, orderFields, offset, limit, false, tenantId);
  }

  @Override
  public Future<Optional<SourceRecord>> getSourceRecordById(String id, String idType, String tenantId) {
    // NOTE: will fail if idType is anything but INSTANCE or RECORD
    ExternalIdType externalIdType = ExternalIdType.valueOf(idType);
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
  public Future<Record> getFormattedRecord(String externalIdIdentifier, String id, String tenantId) {
    // NOTE: will fail if idType is anything but INSTANCE or RECORD
    ExternalIdType externalIdType = ExternalIdType.valueOf(externalIdIdentifier);
    return recordDao.getRecordByExternalId(id, externalIdType, tenantId)
      .map(optionalRecord -> formatMarcRecord(optionalRecord.orElseThrow(() ->
        new NotFoundException(format("Couldn't find Record with %s id %s", externalIdIdentifier, id)))));
  }

  @Override
  public Future<Boolean> updateSuppressFromDiscoveryForRecord(SuppressFromDiscoveryDto suppressFromDiscoveryDto, String tenantId) {
    return recordDao.updateSuppressFromDiscoveryForRecord(suppressFromDiscoveryDto, tenantId);
  }

  @Override
  public Future<Boolean> deleteRecordsBySnapshotId(String snapshotId, String tenantId) {
    return recordDao.deleteRecordsBySnapshotId(snapshotId, tenantId);
  }

  @Override
  public Future<Record> updateSourceRecord(ParsedRecordDto parsedRecordDto, String snapshotId, String tenantId) {
    return recordDao.updateSourceRecord(parsedRecordDto, snapshotId, tenantId);
  }

  private Record ensureRecordForeignKeys(Record record) {
    if (Objects.nonNull(record.getRawRecord()) && StringUtils.isEmpty(record.getRawRecord().getId())) {
      record.getRawRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getParsedRecord()) && StringUtils.isEmpty(record.getParsedRecord().getId())) {
      record.getParsedRecord().setId(record.getId());
    }
    if (Objects.nonNull(record.getErrorRecord()) && StringUtils.isEmpty(record.getErrorRecord().getId())) {
      record.getErrorRecord().setId(record.getId());
    }
    return record;
  }

  private Record validateParsedRecordId(Record record) {
    if (Objects.isNull(record.getParsedRecord()) && Objects.isNull(record.getParsedRecord().getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
    return record;
  }

  private Record formatMarcRecord(Record record) {
    try {
      String parsedRecordContent;
      if (record.getParsedRecord().getContent() instanceof String) {
        parsedRecordContent = (String) record.getParsedRecord().getContent();
      } else {
        parsedRecordContent = JsonObject.mapFrom(record.getParsedRecord().getContent()).encode();
      }
      record.getParsedRecord().setFormattedContent(MarcUtil.marcJsonToTxtMarc(parsedRecordContent));
    } catch (IOException e) {
      LOG.error("Couldn't format MARC record", e);
    }
    return record;
  }

}