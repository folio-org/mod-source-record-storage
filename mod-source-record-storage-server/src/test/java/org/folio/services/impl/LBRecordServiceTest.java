package org.folio.services.impl;

import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBRecordMocks;
import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.services.LBRecordService;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.core.Promise;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractEntityServiceTest<Record, RecordCollection, RecordQuery, LBRecordDao, LBRecordService, LBRecordMocks> {

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(LBRecordDao.class);
    service = new LBRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);

    LBSnapshotDao snapshotDao = Mockito.mock(LBSnapshotDao.class);

    mocks.getMockEntities().forEach(record -> {
      Promise<Optional<Snapshot>> getByIdPromise = Promise.promise();
      Optional<Snapshot> snapshot = TestMocks.getSnapshot(record.getSnapshotId());
      when(snapshotDao.getById(snapshot.get().getJobExecutionId(), TENANT_ID)).thenReturn(getByIdPromise.future());
      getByIdPromise.complete(snapshot);
      Promise<Integer> calculateGenerationPromise = Promise.promise();
      when(mockDao.calculateGeneration(record, TENANT_ID)).thenReturn(calculateGenerationPromise.future());
      calculateGenerationPromise.complete(0);
    });

    Record invalidRecord = mocks.getInvalidMockEntity();
    Promise<Optional<Snapshot>> getByIdPromise = Promise.promise();
    Optional<Snapshot> snapshot = TestMocks.getSnapshot(invalidRecord.getSnapshotId());
    when(snapshotDao.getById(snapshot.get().getJobExecutionId(), TENANT_ID)).thenReturn(getByIdPromise.future());
    getByIdPromise.complete(snapshot);
    Promise<Integer> calculateGenerationPromise = Promise.promise();
    when(mockDao.calculateGeneration(invalidRecord, TENANT_ID)).thenReturn(calculateGenerationPromise.future());
    calculateGenerationPromise.complete(0);

    FieldUtils.writeField(service, "snapshotDao", snapshotDao, true);
  }

  @Override
  public LBRecordMocks getMocks() {
    return LBRecordMocks.mock();
  }

}