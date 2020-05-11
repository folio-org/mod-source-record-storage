package org.folio.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.folio.EntityMocks;
import org.folio.dao.EntityDao;
import org.folio.dao.query.EntityQuery;
import org.junit.Test;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractEntityDaoTest<E, C, Q extends EntityQuery, D extends EntityDao<E, C, Q>, M extends EntityMocks<E, C, Q>>
    extends AbstractDaoTest {

  D dao;

  M mocks = initMocks();

  @Test
  public void shouldGetById(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getById(dao.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        mocks.compareEntities(context, mocks.getMockEntity(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldNotFindWhenGetById(TestContext context) {
    Async async = context.async();
    dao.getById(dao.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldGetByNoopQuery(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntities(), TENANT_ID).onComplete(create -> {
      if (create.failed()) {
        context.fail(create.cause());
      }
      dao.getByQuery(mocks.getNoopQuery(), 0, 10, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        mocks.assertNoopQueryResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByArbitruaryQuery(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntities(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByQuery(mocks.getArbitruaryQuery(), 0, 10, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        mocks.assertArbitruaryQueryResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByArbitruarySortedQuery(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntities(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByQuery(mocks.getArbitruarySortedQuery(), 0, 10, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        mocks.assertArbitruarySortedQueryResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSave(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, mocks.getMockEntity(), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldErrorWhileTryingToSave(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getInvalidMockEntity(), TENANT_ID).onComplete(res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void shouldUpdate(TestContext context) {
    Async async = context.async();
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      E mockUpdateEntity = mocks.getUpdatedMockEntity();
      dao.update(mockUpdateEntity, TENANT_ID).onComplete(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        mocks.compareEntities(context, mockUpdateEntity, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    Async async = context.async();
    E mockUpdateEntity = mocks.getUpdatedMockEntity();
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
    dao.save(mocks.getMockEntity(), TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.delete(dao.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
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
    dao.delete(dao.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
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
    dao.save(mocks.getMockEntities(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<E> actual = new ArrayList<>();
      dao.getByQuery(mocks.getNoopQuery(), 0, 10, TENANT_ID, entity -> {
        actual.add(entity);
      }, finished -> {
        if (finished.failed()) {
          context.fail(finished.cause());
        }
        mocks.compareEntities(context, mocks.getMockEntities(), actual);
        async.complete();
      });
    });
  }

  @Test
  public void shouldGenerateWhereClauseFromQuery(TestContext context) {
    Q filter = mocks.getCompleteQuery();
    context.assertEquals(mocks.getCompleteWhereClause().trim(), filter.toWhereClause().trim());
  }

  public abstract M initMocks();

}
