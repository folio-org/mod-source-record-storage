package org.folio.services.handlers.match;

import org.folio.dao.RecordDao;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.TypeConnection;
import org.jooq.Condition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalHrid;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByExternalId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByRecordId;
import static org.folio.dao.util.RecordDaoUtil.filterRecordByState;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;

/**
 * Handler for MARC-MARC matching/not-matching MARC bibliographic record by specific fields.
 */
@Component
public class MarcBibliographicMatchEventHandler extends AbstractMarcMatchEventHandler {

  @Autowired
  public MarcBibliographicMatchEventHandler(RecordDao recordDao) {
    super(TypeConnection.MARC_BIB, recordDao, DI_SRS_MARC_BIB_RECORD_MATCHED, DI_SRS_MARC_BIB_RECORD_NOT_MATCHED);
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value();
  }

}
