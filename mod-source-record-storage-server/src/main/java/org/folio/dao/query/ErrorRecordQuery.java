package org.folio.dao.query;

import static org.folio.dao.impl.ErrorRecordDaoImpl.DESCRIPTION_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.ErrorRecord;

public class ErrorRecordQuery extends ErrorRecord implements EntityQuery {

  private final Set<OrderBy> sort = new HashSet<>();

  private final Map<String, String> propertyToColumn;

  public ErrorRecordQuery() {
    Map<String, String> propertyToColumn = new HashMap<>();
    propertyToColumn.put("id", ID_COLUMN_NAME);
    propertyToColumn.put("content", CONTENT_COLUMN_NAME);
    propertyToColumn.put("description", DESCRIPTION_COLUMN_NAME);
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
    return WhereClauseBuilder.of()
      .append(getId(), ID_COLUMN_NAME)
      .append(getDescription(), DESCRIPTION_COLUMN_NAME)
      .build();
  }

  @Override
  public boolean equals(Object other) {
    return Objects.nonNull(other) && DaoUtil.equals(sort, ((ErrorRecordQuery) other).getSort()) && super.equals(other);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}