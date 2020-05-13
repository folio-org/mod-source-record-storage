package org.folio.dao.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.folio.dao.EntityDao;
import org.folio.dao.query.EntityQuery;
import org.junit.Test;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractEntityDaoTest<E, C, Q extends EntityQuery, DAO extends EntityDao<E, C, Q>> extends AbstractDaoTest {

  DAO dao;

  @Test
  public void shouldGetById(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getById(dao.getId(getMockEntity()), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        compareEntities(context, getMockEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldNotFindWhenGetById(TestContext context) {
    Async async = context.async();
    dao.getById(dao.getId(getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldGetByEmptyResults(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntities(), TENANT_ID).onComplete(create -> {
      if (create.failed()) {
        context.fail(create.cause());
      }
      dao.getByQuery(getNoopQuery(), 0, 0, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertEmptyResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByNoopQuery(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntities(), TENANT_ID).onComplete(create -> {
      if (create.failed()) {
        context.fail(create.cause());
      }
      dao.getByQuery(getNoopQuery(), 0, 10, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertNoopQueryResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByArbitruaryQuery(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntities(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByQuery(getArbitruaryQuery(), 0, 10, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertArbitruaryQueryResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByArbitruarySortedQuery(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntities(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByQuery(getArbitruarySortedQuery(), 0, 10, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertArbitruarySortedQueryResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSave(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntity(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareEntities(context, getMockEntity(), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldErrorWhileTryingToSave(TestContext context) {
    Async async = context.async();
    dao.save(getInvalidMockEntity(), TENANT_ID).onComplete(res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void shouldUpdate(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      E mockUpdateEntity = getUpdatedMockEntity();
      dao.update(mockUpdateEntity, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        compareEntities(context, mockUpdateEntity, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    Async async = context.async();
    E mockUpdateEntity = getUpdatedMockEntity();
    dao.update(mockUpdateEntity, TENANT_ID).onComplete(res -> {
      context.assertTrue(res.failed());
      String expectedMessage = String.format("%s row with id %s was not updated", dao.getTableName(), dao.getId(mockUpdateEntity));
      context.assertEquals(expectedMessage, res.cause().getMessage());
      async.complete();
    });
  }

  @Test
  public void shouldDelete(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.delete(dao.getId(getMockEntity()), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldNotDelete(TestContext context) {
    Async async = context.async();
    dao.delete(dao.getId(getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result());
      async.complete();
    });
  }

  @Test
  public void shouldStreamGetByQuery(TestContext context) {
    Async async = context.async();
    dao.save(getMockEntities(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<E> actual = new ArrayList<>();
      dao.getByQuery(getNoopQuery(), 0, 10, TENANT_ID, entity -> {
        actual.add(entity);
      }, finished -> {
        if (finished.failed()) {
          context.fail(finished.cause());
        }
        compareEntities(context, getMockEntities(), actual);
        async.complete();
      });
    });
  }

  @Test
  public void shouldGenerateWhereClauseFromQuery(TestContext context) {
    Q filter = getCompleteQuery();
    assertEquals(getCompleteWhereClause().trim(), filter.toWhereClause().trim());
  }

  public void compareEntities(TestContext context, List<E> expected, List<E> actual) {
    Collections.sort(actual, (b1, b2) -> dao.getId(b1).compareTo(dao.getId(b2)));
    for (int i = 0; i < expected.size() - 1; i++) {
      compareEntities(context, expected.get(i), actual.get(i));
    }
  }

  public abstract Q getNoopQuery();

  public abstract Q getArbitruaryQuery();

  public abstract Q getArbitruarySortedQuery();

  public abstract Q getCompleteQuery();

  public abstract E getMockEntity();

  public abstract E getInvalidMockEntity();

  public abstract E getUpdatedMockEntity();

  public abstract List<E> getMockEntities();

  public abstract void compareEntities(TestContext context, E expected, E actual);

  public abstract void assertEmptyResults(TestContext context, C actual);

  public abstract void assertNoopQueryResults(TestContext context, C actual);

  public abstract void assertArbitruaryQueryResults(TestContext context, C actual);

  public abstract void assertArbitruarySortedQueryResults(TestContext context, C actual);

  public abstract String getCompleteWhereClause();

}
