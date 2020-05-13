package org.folio;

import java.util.List;

import org.folio.dao.query.EntityQuery;

import io.vertx.ext.unit.TestContext;

public interface EntityMocks<E, C, Q extends EntityQuery> {

  public String getId(E entity);

  public Q getNoopQuery();

  public Q getArbitruaryQuery();

  public Q getArbitruarySortedQuery();

  public Q getCompleteQuery();

  public E getMockEntity();

  public E getInvalidMockEntity();

  public E getUpdatedMockEntity();

  public List<E> getMockEntities();

  public String getCompleteWhereClause();

  public E getExpectedEntity();

  public E getExpectedUpdatedEntity();

  public List<E> getExpectedEntities();

  public List<E> getExpectedEntitiesForArbitraryQuery();

  public List<E> getExpectedEntitiesForArbitrarySortedQuery();

  public C getExpectedCollection();

  public C getExpectedCollectionForArbitraryQuery();

  public C getExpectedCollectionForArbitrarySortedQuery();

  public void assertEmptyResult(TestContext context, int expectedTotal, C actual);

  public void compareCollections(TestContext context, C expected, C actual);

  public void compareEntities(TestContext context, E expected, E actual);

  public default void compareEntities(TestContext context, List<E> expected, List<E> actual) {
    for (int i = 0; i < expected.size() - 1; i++) {
      compareEntities(context, expected.get(i), actual.get(i));
    }
  }

}