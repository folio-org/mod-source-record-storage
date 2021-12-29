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
  private static final String MATCHED_MARC_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private static final String MATCHED_ID_MARC_FIELD = "999ffs";
  private static final String INSTANCE_ID_MARC_FIELD = "999ffi";
  private static final String INSTANCE_HRID_MARC_FIELD = "001";

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

  /**
   * Builds Condition for filtering by specific field.
   *
   * @param valueFromField - value by which will be filtered from DB.
   * @param fieldPath      - resulted fieldPath
   * @return - built Condition
   */
  @Override
  protected Condition buildConditionBasedOnMarcField(String valueFromField, String fieldPath) {
    Condition condition;
    switch (fieldPath) {
      case MATCHED_ID_MARC_FIELD:
        condition = filterRecordByRecordId(valueFromField).and(filterRecordByState(Record.State.ACTUAL.value()));
        break;
      case INSTANCE_ID_MARC_FIELD:
        condition = filterRecordByExternalId(valueFromField).and(filterRecordByState(Record.State.ACTUAL.value()));
        break;
      case INSTANCE_HRID_MARC_FIELD:
        condition = filterRecordByExternalHrid(valueFromField).and(filterRecordByState(Record.State.ACTUAL.value()));
        break;
      default:
        condition = null;
    }
    return condition;
  }

  @Override
  protected String getMatchedMarcKey() {
    return MATCHED_MARC_KEY;
  }
}
