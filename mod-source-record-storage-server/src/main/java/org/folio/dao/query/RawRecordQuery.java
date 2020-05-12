package org.folio.dao.query;

import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.RawRecord;

public class RawRecordQuery extends RawRecord implements EntityQuery {

  private static final Map<String, String> propertyToColumn = DaoUtil.getImmutableContentPropertyToColumnMap();

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
      .build();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof RawRecordQuery)) {
      return false;
    }
    RawRecordQuery rhs = ((RawRecordQuery) other);
    return new EqualsBuilder()
      .append(getId(), rhs.getId())
      .append(getSort(), rhs.getSort())
      .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
      .append(getId())
      .append(getSort())
      .toHashCode();
  }

}