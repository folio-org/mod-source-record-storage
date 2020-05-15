package org.folio.dao.query;

import java.util.Map;

public abstract class AbstractEntityQuery implements EntityQuery {

  private final Map<String, String> propertyToColumn;

  private final Class<?> queryFor;

  private final QueryBuilder builder;

  public AbstractEntityQuery(Map<String, String> propertyToColumn, Class<?> queryFor) {
    this.propertyToColumn = propertyToColumn;
    this.queryFor = queryFor;
    this.builder = QueryBuilder.builder(this);
  }

  @Override
  public Map<String, String> getPropertyToColumn() {
    return propertyToColumn;
  }

  @Override
  public Class<?> queryFor() {
    return queryFor;
  }

  @Override
  public QueryBuilder builder() {
    return builder;
  }

}