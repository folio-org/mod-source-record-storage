package org.folio.dao.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ErrorRecord;

public class ErrorRecordFilter extends ErrorRecord implements BeanFilter {

  @Override
  public String toWhereClause() {
    List<String> statements = new ArrayList<>();
    if (StringUtils.isNotEmpty(getId())) {
      statements.add(String.format("id = '%s'", getId()));
    }
    if (StringUtils.isNotEmpty(getDescription())) {
      statements.add(String.format("description = '%s'", getDescription()));
    }
    return statements.isEmpty() ? StringUtils.EMPTY : "WHERE " + String.join(" AND ", statements);
  }

}