package org.folio.services.impl;

import static java.lang.String.format;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBSnapshotMocks;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.services.LBSnapshotService;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBSnapshotServiceTest extends AbstractEntityServiceTest<Snapshot, SnapshotCollection, SnapshotQuery, LBSnapshotDao, LBSnapshotService, LBSnapshotMocks> {

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(LBSnapshotDao.class);
    service = new LBSnapshotServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
  }

  @Override
  public void shouldUpdate(TestContext context) {
    Promise<Snapshot> updatePromise = Promise.promise();
    Promise<Optional<Snapshot>> getByIdPromise = Promise.promise();
    Snapshot mockUpdateEntity = mocks.getUpdatedMockEntity();
    when(mockDao.getById(mockUpdateEntity.getJobExecutionId(), TENANT_ID)).thenReturn(getByIdPromise.future());
    when(mockDao.update(mockUpdateEntity, TENANT_ID)).thenReturn(updatePromise.future());
    Async async = context.async();
    service.update(mockUpdateEntity, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      mocks.compareEntities(context, mocks.getExpectedUpdatedEntity(), res.result());
      async.complete();
    });
    getByIdPromise.complete(Optional.of(mockUpdateEntity));
    updatePromise.complete(mocks.getExpectedUpdatedEntity());
  }

  @Override
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    Promise<Snapshot> updatePromise = Promise.promise();
    Promise<Optional<Snapshot>> getByIdPromise = Promise.promise();
    Snapshot mockUpdateEntity = mocks.getUpdatedMockEntity();
    when(mockDao.getById(mockUpdateEntity.getJobExecutionId(), TENANT_ID)).thenReturn(getByIdPromise.future());
    when(mockDao.update(mockUpdateEntity, TENANT_ID)).thenReturn(updatePromise.future());
    Async async = context.async();
    String expectedMessage = format("%s row with id %s was not updated", mockDao.getTableName(), mocks.getId(mockUpdateEntity));
    service.update(mockUpdateEntity, TENANT_ID).onComplete(res -> {
      context.assertTrue(res.failed());
      context.assertEquals(expectedMessage, res.cause().getMessage());
      async.complete();
    });
    getByIdPromise.complete(Optional.of(mockUpdateEntity));
    updatePromise.fail(expectedMessage);
  }

  @Override
  public LBSnapshotMocks getMocks() {
    return LBSnapshotMocks.mock();
  }

}