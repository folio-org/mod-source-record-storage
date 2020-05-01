package org.folio.dao.filter;

import static org.folio.dao.impl.ErrorRecordDaoImpl.DESCRIPTION_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.ErrorRecord;

public class ErrorRecordFilter extends ErrorRecord implements BeanFilter {

  @Override
  public String toWhereClause() {
    return WhereClauseBuilder.of()
      .append(getId(), ID_COLUMN_NAME)
      .append(getDescription(), DESCRIPTION_COLUMN_NAME)
      .build();
  }

}