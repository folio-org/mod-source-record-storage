package org.folio.services.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBRecordMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.LBRecordService;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractEntityServiceTest<Record, RecordCollection, RecordQuery, LBRecordDao, LBRecordService, LBRecordMocks> {

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(LBRecordDao.class);
    service = new LBRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
  }

  @Override
  public LBRecordMocks getMocks() {
    return LBRecordMocks.mock();
  }

}