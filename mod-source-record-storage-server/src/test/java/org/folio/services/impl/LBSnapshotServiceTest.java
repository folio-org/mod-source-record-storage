package org.folio.services.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBSnapshotMocks;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.services.LBSnapshotService;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

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
  public LBSnapshotMocks getMocks() {
    return LBSnapshotMocks.mock();
  }

}