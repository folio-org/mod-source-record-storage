package org.folio.services.impl;

import static java.lang.String.format;
import static java.util.Objects.isNull;

import java.util.List;

import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.ParsedRecordService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;

@Service
public class ParsedRecordServiceImpl extends AbstractEntityService<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao>
    implements ParsedRecordService {

  @Autowired
  private LBRecordDao recordDao;

  @Override
  public Future<ParsedRecord> updateParsedRecord(Record record, String tenantId) {
    String id = record.getId();
    ParsedRecord parsedRecord = record.getParsedRecord();
    ExternalIdsHolder externalIdsHolder = record.getExternalIdsHolder();
    Metadata metadata = record.getMetadata();
    return dao.inTransaction(tenantId, connection ->
      recordDao.getById(connection, id, tenantId)
        .map(r -> r.orElseThrow(() -> new NotFoundException(format("Couldn't find record with id %s", id))))
        .compose(r -> recordDao.save(connection, r.withExternalIdsHolder(externalIdsHolder).withMetadata(metadata), tenantId))
        .compose(r -> dao.save(connection, parsedRecord, tenantId)));
  }

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId) {
    @SuppressWarnings("squid:S3740")
    List<Future> futures = recordCollection.getRecords().stream()
      .map(this::validateParsedRecordId)
      .map(record -> updateParsedRecord(record, tenantId))
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

  private Record validateParsedRecordId(Record record) {
    if (isNull(record.getParsedRecord().getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
    return record;
  }

}