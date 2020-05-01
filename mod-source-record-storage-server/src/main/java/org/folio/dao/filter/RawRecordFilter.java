package org.folio.dao.filter;

import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.RawRecord;

public class RawRecordFilter extends RawRecord implements BeanFilter {

  @Override
  public String toWhereClause() {
    return WhereClauseBuilder.of()
      .append(getId(), ID_COLUMN_NAME)
      .build();
  }

}