package org.folio.dao.query;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.Record;

public class RecordQuery extends Record implements EntityQuery {

  private final Set<OrderBy> sort = new HashSet<>();

  private final Map<String, String> propertyToColumn = ImmutableMap.copyOf(new HashMap<String, String>() {
    private static final long serialVersionUID = -3715788293589367631L;
    {
      put("id", ID_COLUMN_NAME);
      put("snapshotId", SNAPSHOT_ID_COLUMN_NAME);
      put("matchedProfileId", MATCHED_PROFILE_ID_COLUMN_NAME);
      put("matchedId", MATCHED_ID_COLUMN_NAME);
      put("generation", GENERATION_COLUMN_NAME);
      put("recordType", RECORD_TYPE_COLUMN_NAME);
      put("order", ORDER_IN_FILE_COLUMN_NAME);
      put("externalIdsHolder.instanceId", INSTANCE_ID_COLUMN_NAME);
      put("additionalInfo.suppressDiscovery", SUPPRESS_DISCOVERY_COLUMN_NAME);
      put("state", STATE_COLUMN_NAME);
      put("metadata.createdByUserId", CREATED_BY_USER_ID_COLUMN_NAME);
      put("metadata.createdDate", CREATED_DATE_COLUMN_NAME);
      put("metadata.updatedByUserId", UPDATED_BY_USER_ID_COLUMN_NAME);
      put("metadata.updatedDate", UPDATED_DATE_COLUMN_NAME);
    }
  });

  @Override
  public Set<OrderBy> getSort() {
    return sort;
  }

  @Override
  public Map<String, String> getPropertyToColumn() {
    return propertyToColumn;
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
    if (Objects.nonNull(getRecordType())) {
      whereClauseBuilder
        .append(getRecordType().toString(), RECORD_TYPE_COLUMN_NAME);
    }
    if (Objects.nonNull(getState())) {
      whereClauseBuilder
        .append(getState().toString(), STATE_COLUMN_NAME);
    }
    if (Objects.nonNull(getExternalIdsHolder())) {
      whereClauseBuilder
        .append(getExternalIdsHolder().getInstanceId(), INSTANCE_ID_COLUMN_NAME);
    }
    if (Objects.nonNull(getAdditionalInfo())) {
      whereClauseBuilder
        .append(getAdditionalInfo().getSuppressDiscovery(), SUPPRESS_DISCOVERY_COLUMN_NAME);
    }
    if (Objects.nonNull(getMetadata())) {
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

  @Override
  public boolean equals(Object other) {
    return DaoUtil.equals(this, other) && super.equals(other);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}