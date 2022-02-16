package org.folio.services.handlers.actions;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.RecordService;
import org.folio.services.util.TypeConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;

@Component
public class MarcAuthorityDeleteEventHandler extends AbstractDeleteEventHandler {

  @Autowired
  public MarcAuthorityDeleteEventHandler(RecordService recordService) {
    super(recordService, TypeConnection.MARC_AUTHORITY);
  }

  @Override
  protected Future<Boolean> deleteRecord(Record record, String tenantId) {
    return recordService.deleteRecordById(record.getId(), tenantId);
  }

  @Override
  protected String getNextEventType() {
    return DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value();
  }
}
