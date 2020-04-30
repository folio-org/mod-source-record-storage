package org.folio.dao.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.folio.dao.BeanDao;
import org.folio.dao.filter.BeanFilter;
import org.junit.Test;

import io.vertx.core.CompositeFuture;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractBeanDaoTest<I, C, F extends BeanFilter, DAO extends BeanDao<I, C, F>> extends AbstractDaoTest {

  static final String DELETE_SQL_TEMPLATE = "DELETE FROM %s;";

  DAO dao;

  @Override
  public void deleteRows(TestContext context) {
    Async async = context.async();
    String sql = String.format(DELETE_SQL_TEMPLATE, dao.getTableName());
    dao.getPostgresClient(TENANT_ID).execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void testGetById(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getById(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertTrue(res.result().isPresent());
        compareBeans(getMockBean(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void testGetByIdNotFound(TestContext context) {
    Async async = context.async();
    dao.getById(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      assertFalse(res.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void testGetByNoopFilter(TestContext context) {
    Async async = context.async();
    I[] beans = getMockBeans();
    CompositeFuture.all(
      dao.save(beans[0], TENANT_ID),
      dao.save(beans[1], TENANT_ID),
      dao.save(beans[2], TENANT_ID),
      dao.save(beans[3], TENANT_ID)
    ).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByFilter(getNoopFilter(), 0, 10, TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertNoopFilterResults(res.result());
        async.complete();
      });
    });
  }

  @Test
  public void testGetByArbitruaryFilter(TestContext context) {
    Async async = context.async();
    I[] beans = getMockBeans();
    CompositeFuture.all(
      dao.save(beans[0], TENANT_ID),
      dao.save(beans[1], TENANT_ID),
      dao.save(beans[2], TENANT_ID),
      dao.save(beans[3], TENANT_ID)
    ).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByFilter(getArbitruaryFilter(), 0, 10, TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertArbitruaryFilterResults(res.result());
        async.complete();
      });
    });
  }

  @Test
  public void testSave(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareBeans(getMockBean(), res.result());
      async.complete();
    });
  }

  @Test
  public void testSaveGenerateId(TestContext context) {
    Async async = context.async();
    dao.save(getMockBeanWithoutId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareBeans(getMockBeanWithoutId(), res.result());
      async.complete();
    });
  }

  @Test
  public void testSaveError(TestContext context) {
    Async async = context.async();
    dao.save(getInvalidMockBean(), TENANT_ID).setHandler(res -> {
      assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void testUpdate(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      I mockUpdateBean = getUpdatedMockBean();
      dao.update(mockUpdateBean, TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        compareBeans(mockUpdateBean, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void testUpdateError(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      I mockInvalidUpdateBean = getInvalidUpdatedMockBean();
      dao.update(mockInvalidUpdateBean, TENANT_ID).setHandler(res -> {
        assertTrue(res.failed());
        async.complete();
      });
    });
  }

  @Test
  public void testUpdateNotFound(TestContext context) {
    Async async = context.async();
    I mockUpdateBean = getUpdatedMockBean();
    dao.update(mockUpdateBean, TENANT_ID).setHandler(res -> {
      assertTrue(res.failed());
      String expectedMessage = String.format("%s row with id %s was not updated", dao.getTableName(), dao.getId(mockUpdateBean));
      assertEquals(expectedMessage, res.cause().getMessage());
      async.complete();
    });
  }

  @Test
  public void testDelete(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.delete(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertTrue(res.result());
        async.complete();
      });
    });
  }

  @Test
  public void testDeleteNotFound(TestContext context) {
    Async async = context.async();
    dao.delete(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      assertFalse(res.result());
      async.complete();
    });
  }

  public abstract F getNoopFilter();

  public abstract F getArbitruaryFilter();

  public abstract I getMockBean();

  public abstract I getMockBeanWithoutId();

  public abstract I getInvalidMockBean();

  public abstract I getUpdatedMockBean();

  public abstract I getInvalidUpdatedMockBean();

  public abstract I[] getMockBeans();

  public abstract void compareBeans(I expected, I actual);

  public abstract void assertTotal(Integer expected, C actual);

  public abstract void assertNoopFilterResults(C actual);

  public abstract void assertArbitruaryFilterResults(C actual);

}
