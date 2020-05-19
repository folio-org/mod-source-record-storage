package org.folio.dao.query;

public abstract class AbstractEntityQuery<Q extends EntityQuery<Q>> implements EntityQuery<Q> {

  private final QueryBuilder<Q> builder = new QueryBuilder<>((Q) this);

  @Override
  public QueryBuilder<Q> builder() {
    return builder;
  }

}