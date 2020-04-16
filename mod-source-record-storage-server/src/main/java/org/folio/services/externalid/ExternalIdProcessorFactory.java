package org.folio.services.externalid;

import org.folio.dao.RecordDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ExternalIdProcessorFactory {

  @Autowired
  private RecordDao recordDao;

  public ExternalIdProcessor createExternalIdProcessor(String idType) {
    switch (idType) {  //NOSONAR
      case "INSTANCE":
        return new InstanceExternalIdProcessor(recordDao);
      default:
        return new RecordExternalIdProcessor(recordDao);
    }
  }
}
