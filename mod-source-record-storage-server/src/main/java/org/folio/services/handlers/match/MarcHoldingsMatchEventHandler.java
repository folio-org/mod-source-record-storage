package org.folio.services.handlers.match;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED;

import org.springframework.beans.factory.annotation.Autowired;

import org.folio.dao.RecordDao;
import org.folio.services.util.TypeConnection;

public class MarcHoldingsMatchEventHandler extends AbstractMarcMatchEventHandler {

  @Autowired
  public MarcHoldingsMatchEventHandler(RecordDao recordDao) {
    super(TypeConnection.MARC_HOLDINGS, recordDao, DI_SRS_MARC_HOLDINGS_RECORD_MATCHED,
      DI_SRS_MARC_HOLDINGS_RECORD_NOT_MATCHED);
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

}
