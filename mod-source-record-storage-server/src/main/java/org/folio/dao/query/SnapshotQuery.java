package org.folio.dao.query;

import static org.folio.dao.impl.LBSnapshotDaoImpl.PROCESSING_STARTED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBSnapshotDaoImpl.STATUS_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.Snapshot;

public class SnapshotQuery extends Snapshot implements EntityQuery {

  private final Set<OrderBy> sort = new HashSet<>();

  private final Map<String, String> propertyToColumn;

  public SnapshotQuery() {
    Map<String, String> propertyToColumn = new HashMap<>();
    propertyToColumn.put("jobExecutionId", ID_COLUMN_NAME);
    propertyToColumn.put("status", STATUS_COLUMN_NAME);
    propertyToColumn.put("processingStartedDate", PROCESSING_STARTED_DATE_COLUMN_NAME);
    this.propertyToColumn = ImmutableMap.copyOf(propertyToColumn);
  }

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
    WhereClauseBuilder whereClauseBuilder = WhereClauseBuilder.of().append(getJobExecutionId(), ID_COLUMN_NAME)
        .append(getProcessingStartedDate(), PROCESSING_STARTED_DATE_COLUMN_NAME);
    if (Objects.nonNull(getStatus())) {
      whereClauseBuilder.append(getStatus().toString(), STATUS_COLUMN_NAME);
    }
    return whereClauseBuilder.build();
  }

  @Override
  public boolean equals(Object other) {
    return DaoUtil.equals(sort, ((SnapshotQuery) other).getSort()) && super.equals(other);
  }

}