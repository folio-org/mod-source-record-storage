package org.folio.dao.query;

import static org.folio.dao.util.DaoUtil.CONTENT_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.folio.dao.util.DaoUtil;
import org.folio.dao.util.WhereClauseBuilder;
import org.folio.rest.jaxrs.model.ParsedRecord;

public class ParsedRecordQuery extends ParsedRecord implements EntityQuery {

  private final Set<OrderBy> sort = new HashSet<>();

  private final Map<String, String> propertyToColumn;

  public ParsedRecordQuery() {
    Map<String, String> propertyToColumn = new HashMap<>();
    propertyToColumn.put("id", ID_COLUMN_NAME);
    propertyToColumn.put("content", CONTENT_COLUMN_NAME);
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
      .build();
  }

  @Override
  public boolean equals(Object other) {
    return DaoUtil.equals(sort, ((ParsedRecordQuery) other).getSort()) && super.equals(other);
  }

}