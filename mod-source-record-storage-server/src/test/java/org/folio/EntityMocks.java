package org.folio;

import java.util.Collections;
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

  public void compareEntities(TestContext context, E expected, E actual);

  public void assertNoopQueryResults(TestContext context, C actual);

  public void assertArbitruaryQueryResults(TestContext context, C actual);

  public void assertArbitruarySortedQueryResults(TestContext context, C actual);

  public String getCompleteWhereClause();

  public default void compareEntities(TestContext context, List<E> expected, List<E> actual) {
    Collections.sort(actual, (b1, b2) -> getId(b1).compareTo(getId(b2)));
    for (int i = 0; i < expected.size() - 1; i++) {
      compareEntities(context, expected.get(i), actual.get(i));
    }
  }

}