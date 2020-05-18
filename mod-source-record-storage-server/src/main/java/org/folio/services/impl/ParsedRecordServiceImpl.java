package org.folio.services.impl;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;

import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.ParsedRecordsBatchResponse;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.ParsedRecordService;
import org.springframework.stereotype.Service;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;

@Service
public class ParsedRecordServiceImpl extends AbstractEntityService<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao> implements ParsedRecordService {

  @Override
  public Future<ParsedRecordsBatchResponse> updateParsedRecords(RecordCollection recordCollection, String tenantId) {
    List<Future> futures = recordCollection.getRecords().stream()
      .peek(record -> validateParsedRecordId(record.getParsedRecord()))
      .map(record -> dao.update(record.getParsedRecord(), tenantId))
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

  private void validateParsedRecordId(ParsedRecord record) {
    if (Objects.isNull(record.getId())) {
      throw new BadRequestException("Each parsed record should contain an id");
    }
  }

}