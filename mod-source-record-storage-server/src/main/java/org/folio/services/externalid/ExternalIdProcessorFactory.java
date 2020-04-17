package org.folio.services.externalid;

import org.folio.dao.RecordDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Factory for creating specific external id processors
 */
@Component
public class ExternalIdProcessorFactory {

  @Autowired
  private RecordDao recordDao;

  /**
   * Created specific external id processor based on idType
   * @param idType - type id from request
   * @return ExternalIdProcessor - processor for searching source record
   */
  public ExternalIdProcessor createExternalIdProcessor(String idType) {
    switch (idType.toUpperCase()) {  //NOSONAR
      case "INSTANCE":
        return new InstanceExternalIdProcessor(recordDao);
      default:
        return new RecordExternalIdProcessor(recordDao);
    }
  }
}
