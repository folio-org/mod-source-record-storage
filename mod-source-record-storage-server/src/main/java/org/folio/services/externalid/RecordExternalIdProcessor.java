package org.folio.services.externalid;

import java.util.Optional;

import org.folio.dao.RecordDao;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.springframework.stereotype.Component;

import io.vertx.core.Future;

@Component
public class RecordExternalIdProcessor implements ExternalIdProcessor {

  private RecordDao recordDao;

  RecordExternalIdProcessor(RecordDao recordDao){
    this.recordDao = recordDao;
  }

  @Override
  public Future<Optional<SourceRecord>> process(String id, String tenantId) {
    return recordDao.getSourceRecord(id, null, tenantId);
  }
}
