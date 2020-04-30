package org.folio.dao.filter;

import static org.folio.dao.BeanDao.ISO_8601_FORMAT;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Snapshot;

public class SnapshotFilter extends Snapshot implements BeanFilter {

  @Override
  public String toWhereClause() {
    List<String> statements = new ArrayList<>();
    if (StringUtils.isNotEmpty(getJobExecutionId())) {
      statements.add(String.format("id = '%s'", getJobExecutionId()));
    }
    if (getStatus() != null) {
      statements.add(String.format("status = '%s'", getStatus().toString()));
    }
    if (getProcessingStartedDate() != null) {
      statements.add(String.format("processing_started_date = '%s'", ISO_8601_FORMAT.format(getProcessingStartedDate())));
    }
    return statements.isEmpty() ? StringUtils.EMPTY : "WHERE " + String.join(" AND ", statements);
  }

}