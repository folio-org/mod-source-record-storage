package org.folio.dao.filter;

import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.CREATED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.GENERATION_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.INSTANCE_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.MATCHED_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.MATCHED_PROFILE_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.ORDER_IN_FILE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.RECORD_TYPE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SNAPSHOT_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.STATE_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.SUPPRESS_DISCOVERY_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_BY_USER_ID_COLUMN_NAME;
import static org.folio.dao.impl.LBRecordDaoImpl.UPDATED_DATE_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.Record;

public class RecordFilter extends Record implements BeanFilter {

  public RecordFilter() {
    setState(null);
  }

  @Override
  public String toWhereClause() {
    WhereClauseBuilder whereClauseBuilder = WhereClauseBuilder.of()
      .append(getId(), ID_COLUMN_NAME)
      .append(getMatchedId(), MATCHED_ID_COLUMN_NAME)
      .append(getSnapshotId(), SNAPSHOT_ID_COLUMN_NAME)
      .append(getMatchedProfileId(), MATCHED_PROFILE_ID_COLUMN_NAME)
      .append(getGeneration(), GENERATION_COLUMN_NAME)
      .append(getOrder(), ORDER_IN_FILE_COLUMN_NAME);
    if (getRecordType() != null) {
      whereClauseBuilder
        .append(getRecordType().toString(), RECORD_TYPE_COLUMN_NAME);
    }
    if (getState() != null) {
      whereClauseBuilder
        .append(getState().toString(), STATE_COLUMN_NAME);
    }
    if (getExternalIdsHolder() != null) {
      whereClauseBuilder
        .append(getExternalIdsHolder().getInstanceId(), INSTANCE_ID_COLUMN_NAME);
    }
    if (getAdditionalInfo() != null) {
      whereClauseBuilder
        .append(getAdditionalInfo().getSuppressDiscovery(), SUPPRESS_DISCOVERY_COLUMN_NAME);
    }
    if (getMetadata() != null) {
      whereClauseBuilder
        .append(getMetadata().getCreatedByUserId(), CREATED_BY_USER_ID_COLUMN_NAME);
      whereClauseBuilder
        .append(getMetadata().getCreatedDate(), CREATED_DATE_COLUMN_NAME);
      whereClauseBuilder
        .append(getMetadata().getUpdatedByUserId(), UPDATED_BY_USER_ID_COLUMN_NAME);
      whereClauseBuilder
        .append(getMetadata().getUpdatedDate(), UPDATED_DATE_COLUMN_NAME);
    }
    return whereClauseBuilder.build();
  }

}