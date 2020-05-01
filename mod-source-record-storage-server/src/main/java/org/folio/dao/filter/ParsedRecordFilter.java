package org.folio.dao.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.ParsedRecord;

public class ParsedRecordFilter extends ParsedRecord implements BeanFilter {

  @Override
  public String toWhereClause() {
    List<String> statements = new ArrayList<>();
    if (StringUtils.isNotEmpty(getId())) {
      statements.add(String.format("id = '%s'", getId()));
    }
    return statements.isEmpty() ? StringUtils.EMPTY : "WHERE " + String.join(" AND ", statements);
  }

}