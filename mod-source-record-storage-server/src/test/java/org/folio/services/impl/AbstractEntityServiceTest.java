package org.folio.services.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.folio.EntityMocks;
import org.folio.dao.EntityDao;
import org.folio.dao.query.EntityQuery;
import org.folio.services.EntityService;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractEntityServiceTest<E, C, Q extends EntityQuery, D extends EntityDao<E, C, Q>, S extends EntityService<E, C, Q>, M extends EntityMocks<E, C, Q>>
    extends AbstractServiceTest {

  D mockDao;

  S service;

  M mocks = getMocks();

  @Test
  public void shouldGetById(TestContext context) {
    Promise<Optional<E>> getByIdPromise = Promise.promise();
    when(mockDao.getById(mocks.getId(mocks.getMockEntity()), TENANT_ID)).thenReturn(getByIdPromise.future());
    Async async = context.async();
    service.getById(mocks.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result().isPresent());
      mocks.compareEntities(context, mocks.getExpectedEntity(), res.result().get());
      async.complete();
    });
    getByIdPromise.complete(Optional.of(mocks.getExpectedEntity()));
  }

  @Test
  public void shouldNotFindWhenGetById(TestContext context) {
    Promise<Optional<E>> getByIdPromise = Promise.promise();
    when(mockDao.getById(mocks.getId(mocks.getMockEntity()), TENANT_ID)).thenReturn(getByIdPromise.future());
    Async async = context.async();
    service.getById(mocks.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result().isPresent());
      async.complete();
    });
    getByIdPromise.complete(Optional.empty());
  }

  @Test
  public void shouldGetByNoopQuery(TestContext context) {
    Promise<C> getByQueryPromise = Promise.promise();
    Q query = mocks.getNoopQuery();
    when(mockDao.getByQuery(query, 0, 10, TENANT_ID)).thenReturn(getByQueryPromise.future());
    Async async = context.async();
    service.getByQuery(query, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareCollections(context, mocks.getExpectedCollection(), res.result());
      async.complete();
    });
    getByQueryPromise.complete(mocks.getExpectedCollection());
  }

  @Test
  public void shouldGetByArbitruaryQuery(TestContext context) {
    Promise<C> getByQueryPromise = Promise.promise();
    Q query = mocks.getArbitruaryQuery();
    when(mockDao.getByQuery(query, 0, 10, TENANT_ID)).thenReturn(getByQueryPromise.future());
    Async async = context.async();
    service.getByQuery(query, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareCollections(context, mocks.getExpectedCollectionForArbitraryQuery(), res.result());
      async.complete();
    });
    getByQueryPromise.complete(mocks.getExpectedCollectionForArbitraryQuery());
  }

  @Test
  public void shouldGetByArbitruarySortedQuery(TestContext context) {
    Promise<C> getByQueryPromise = Promise.promise();
    Q query = mocks.getArbitruarySortedQuery();
    when(mockDao.getByQuery(query, 0, 10, TENANT_ID)).thenReturn(getByQueryPromise.future());
    Async async = context.async();
    service.getByQuery(query, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareCollections(context, mocks.getExpectedCollectionForArbitrarySortedQuery(), res.result());
      async.complete();
    });
    getByQueryPromise.complete(mocks.getExpectedCollectionForArbitrarySortedQuery());
  }

  @Test
  public void shouldSave(TestContext context) {
    Promise<E> savePromise = Promise.promise();
    when(mockDao.save(mocks.getMockEntity(), TENANT_ID)).thenReturn(savePromise.future());
    Async async = context.async();
    service.save(mocks.getMockEntity(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, mocks.getExpectedEntity(), res.result());
      async.complete();
    });
    savePromise.complete(mocks.getExpectedEntity());
  }

  @Test
  public void shouldErrorWhileTryingToSave(TestContext context) {
    Promise<E> savePromise = Promise.promise();
    E mockEntity = mocks.getInvalidMockEntity();
    when(mockDao.save(mockEntity, TENANT_ID)).thenReturn(savePromise.future());
    Async async = context.async();
    service.save(mockEntity, TENANT_ID).onComplete(res -> {
      context.assertTrue(res.failed());
      async.complete();
    });
    savePromise.fail("Invalid");
  }

  @Test
  public void shouldSaveBatch(TestContext context) {
    Promise<List<E>> savePromise = Promise.promise();
    when(mockDao.save(mocks.getMockEntities(), TENANT_ID)).thenReturn(savePromise.future());
    Async async = context.async();
    service.save(mocks.getMockEntities(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, mocks.getExpectedEntities(), res.result());
      async.complete();
    });
    savePromise.complete(mocks.getExpectedEntities());
  }

  @Test
  public void shouldUpdate(TestContext context) {
    Promise<E> updatePromise = Promise.promise();
    when(mockDao.update(mocks.getUpdatedMockEntity(), TENANT_ID)).thenReturn(updatePromise.future());
    Async async = context.async();
    service.update(mocks.getUpdatedMockEntity(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, mocks.getExpectedUpdatedEntity(), res.result());
      async.complete();
    });
    updatePromise.complete(mocks.getExpectedUpdatedEntity());
  }

  @Test
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    Promise<E> updatePromise = Promise.promise();
    E mockUpdatedEntity = mocks.getUpdatedMockEntity();
    when(mockDao.update(mockUpdatedEntity, TENANT_ID)).thenReturn(updatePromise.future());
    Async async = context.async();
    String expectedMessage = String.format("%s row with id %s was not updated", mockDao.getTableName(), mocks.getId(mockUpdatedEntity));
    service.update(mockUpdatedEntity, TENANT_ID).onComplete(res -> {
      context.assertTrue(res.failed());
      context.assertEquals(expectedMessage, res.cause().getMessage());
      async.complete();
    });
    updatePromise.fail(expectedMessage);
  }

  @Test
  public void shouldDeleteById(TestContext context) {
    Promise<Boolean> deletePromise = Promise.promise();
    when(mockDao.delete(mocks.getId(mocks.getMockEntity()), TENANT_ID)).thenReturn(deletePromise.future());
    Async async = context.async();
    service.delete(mocks.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertTrue(res.result());
      async.complete();
    });
    deletePromise.complete(true);
  }

  @Test
  public void shouldNotDelete(TestContext context) {
    Promise<Boolean> deletePromise = Promise.promise();
    when(mockDao.delete(mocks.getId(mocks.getMockEntity()), TENANT_ID)).thenReturn(deletePromise.future());
    Async async = context.async();
    service.delete(mocks.getId(mocks.getMockEntity()), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertFalse(res.result());
      async.complete();
    });
    deletePromise.complete(false);
  }
  @Test
  public void shouldDeleteByQuery(TestContext context) {
    Promise<Integer> deletePromise = Promise.promise();
    Q query = mocks.getArbitruaryQuery();
    when(mockDao.delete(query, TENANT_ID)).thenReturn(deletePromise.future());
    Async async = context.async();
    service.delete(query, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      context.assertEquals(mocks.getExpectedEntitiesForArbitraryQuery().size(), res.result());
      async.complete();
    });
    deletePromise.complete(mocks.getExpectedEntitiesForArbitraryQuery().size());
  }

  @Test
  public void shouldStreamGetByQuery(TestContext context) {
    Q query = mocks.getNoopQuery();

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        mocks.getExpectedEntities().forEach(entity -> {
          ((Handler<E>) invocation.getArgument(4)).handle(entity);
        });
        ((Handler<Void>) invocation.getArgument(5)).handle(null);
        return null;
      }

    }).when(mockDao).getByQuery(eq(query), eq(0), eq(10), eq(TENANT_ID), any(), any());

    Async async = context.async();
    List<E> actual = new ArrayList<>();
    service.getByQuery(query, 0, 10, TENANT_ID, entity -> {
      context.assertNotNull(entity);
      actual.add(entity);
    }, finished -> {
      mocks.compareEntities(context, mocks.getExpectedEntities(), actual);
      async.complete();
    });
  }

  public abstract M getMocks();

}