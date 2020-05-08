package org.folio.dao.query;

import static org.folio.dao.impl.LBSnapshotDaoImpl.PROCESSING_STARTED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBSnapshotDaoImpl.STATUS_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.drools.core.util.StringUtils;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.Snapshot;

public class SnapshotQuery extends Snapshot implements EntityQuery {

  private final Map<String, String> propertyToColumn = new HashMap<>();

  public SnapshotQuery() {
    propertyToColumn.put("jobExecutionId", ID_COLUMN_NAME);
    propertyToColumn.put("status", STATUS_COLUMN_NAME);
    propertyToColumn.put("processingStartedDate", PROCESSING_STARTED_DATE_COLUMN_NAME);
  }

  @Override
  public String toWhereClause() {
    WhereClauseBuilder whereClauseBuilder = WhereClauseBuilder.of()
      .append(getJobExecutionId(), ID_COLUMN_NAME)
      .append(getProcessingStartedDate(), PROCESSING_STARTED_DATE_COLUMN_NAME);
    if (Objects.nonNull(getStatus())) {
      whereClauseBuilder
        .append(getStatus().toString(), STATUS_COLUMN_NAME);
    }
    return whereClauseBuilder.build();
  }

  @Override
  public String toOrderByClause() {
    return StringUtils.EMPTY;
  }

  @Override
  public Optional<String> getPropertyColumnName(String property) {
    return Optional.ofNullable(propertyToColumn.get(property));
  }

}