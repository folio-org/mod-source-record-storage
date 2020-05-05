package org.folio.dao.filter;

import static org.folio.dao.impl.LBSnapshotDaoImpl.PROCESSING_STARTED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBSnapshotDaoImpl.STATUS_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.Objects;

import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.Snapshot;

public class SnapshotFilter extends Snapshot implements BeanFilter {

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

}