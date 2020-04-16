package org.folio.services.externalid;

import java.util.Optional;

import org.folio.dao.RecordDao;
import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.SourceRecord;

import io.vertx.core.Future;

public class InstanceExternalIdProcessor implements ExternalIdProcessor {

  private RecordDao recordDao;

  InstanceExternalIdProcessor(RecordDao recordDao){
    this.recordDao = recordDao;
  }

  @Override
  public Future<Optional<SourceRecord>> process(String id, String tenantId) {
    return recordDao.getSourceRecord(id, ExternalIdType.INSTANCE, tenantId);
  }
}
