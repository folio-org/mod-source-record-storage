package org.folio.dao.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.folio.dao.BeanDao;
import org.folio.dao.filter.BeanFilter;
import org.junit.Test;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractBeanDaoTest<I, C, F extends BeanFilter, DAO extends BeanDao<I, C, F>> extends AbstractDaoTest {

  DAO dao;

  @Test
  public void shouldGetById(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getById(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        context.assertTrue(res.result().isPresent());
        compareBeans(context, getMockBean(), res.result().get());
        async.complete();
      });
    });
  }

  @Test
  public void shouldNotFindWhenGetById(TestContext context) {
    Async async = context.async();
    dao.getById(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldGetByNoopFilter(TestContext context) {
    Async async = context.async();
    dao.save(getMockBeans(), TENANT_ID).setHandler(create -> {
      if (create.failed()) {
        context.fail(create.cause());
      }
      dao.getByFilter(getNoopFilter(), 0, 10, TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertNoopFilterResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldGetByArbitruaryFilter(TestContext context) {
    Async async = context.async();
    dao.save(getMockBeans(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.getByFilter(getArbitruaryFilter(), 0, 10, TENANT_ID).setHandler(res -> {
        if (res.failed()) {
          context.fail(res.cause());
        }
        assertArbitruaryFilterResults(context, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSave(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareBeans(context, getMockBean(), res.result());
      async.complete();
    });
  }

  @Test
  public void shouldErrorWhileTryingToSave(TestContext context) {
    Async async = context.async();
    dao.save(getInvalidMockBean(), TENANT_ID).setHandler(res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
  }

  @Test
  public void shouldUpdate(TestContext context) {
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
        compareBeans(context, mockUpdateBean, res.result());
        async.complete();
      });
    });
  }

  @Test
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    Async async = context.async();
    I mockUpdateBean = getUpdatedMockBean();
    dao.update(mockUpdateBean, TENANT_ID).setHandler(res -> {
      context.assertTrue(res.failed());
      String expectedMessage = String.format("%s row with id %s was not updated", dao.getTableName(), dao.getId(mockUpdateBean));
      context.assertEquals(expectedMessage, res.cause().getMessage());
      async.complete();
    });
  }

  @Test
  public void shouldDelete(TestContext context) {
    Async async = context.async();
    dao.save(getMockBean(), TENANT_ID).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      dao.delete(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
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
    dao.delete(dao.getId(getMockBean()), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result());
      async.complete();
    });
  }

  @Test
  public void shouldStreamGetByFilter(TestContext context) {
    Async async = context.async();
    dao.save(getMockBeans(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      List<I> actual = new ArrayList<>();
      dao.getByFilter(getNoopFilter(), 0, 10, TENANT_ID, bean -> {
        actual.add(bean);
      }, finished -> {
        if (finished.failed()) {
          context.fail(finished.cause());
        }
        compareBeans(context, getMockBeans(), actual);
        async.complete();
      });
    });
  }

  public void compareBeans(TestContext context, List<I> expected, List<I> actual) {
    Collections.sort(actual, (b1, b2) -> dao.getId(b1).compareTo(dao.getId(b2)));
    for (int i = 0; i < expected.size() - 1; i++) {
      compareBeans(context, expected.get(i), actual.get(i));
    }
  }

  public abstract F getNoopFilter();

  public abstract F getArbitruaryFilter();

  public abstract I getMockBean();

  public abstract I getInvalidMockBean();

  public abstract I getUpdatedMockBean();

  public abstract List<I> getMockBeans();

  public abstract void compareBeans(TestContext context, I expected, I actual);

  public abstract void assertNoopFilterResults(TestContext context, C actual);

  public abstract void assertArbitruaryFilterResults(TestContext context, C actual);

}
