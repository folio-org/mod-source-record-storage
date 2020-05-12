package org.folio.dao.query;

import static org.folio.dao.impl.ErrorRecordDaoImpl.DESCRIPTION_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.ErrorRecord;

public class ErrorRecordQuery extends ErrorRecord implements EntityQuery {

  private static final Map<String, String> propertyToColumn;

  static {
    Map<String, String> ptc = DaoUtil.getBasicContentPropertyToColumnMap();
    ptc.put("description", DESCRIPTION_COLUMN_NAME);
    propertyToColumn = ImmutableMap.copyOf(ptc);
  }

  private final Set<OrderBy> sort = new HashSet<>();

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
    if (other == this) {
      return true;
    }
    if (!(other instanceof ErrorRecordQuery)) {
      return false;
    }
    ErrorRecordQuery rhs = ((ErrorRecordQuery) other);
    return new EqualsBuilder()
      .append(getId(), rhs.getId())
      .append(getDescription(), rhs.getDescription())
      .append(getSort(), rhs.getSort())
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(getId())
      .append(getDescription())
      .append(getSort())
      .toHashCode();
  }

}