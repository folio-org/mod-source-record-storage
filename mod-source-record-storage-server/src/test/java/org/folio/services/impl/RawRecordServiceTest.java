package org.folio.services.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.RawRecordMocks;
import org.folio.dao.RawRecordDao;
import org.folio.dao.query.RawRecordQuery;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.folio.services.RawRecordService;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RawRecordServiceTest extends AbstractEntityServiceTest<RawRecord, RawRecordCollection, RawRecordQuery, RawRecordDao, RawRecordService, RawRecordMocks> {

  @Override
  public void createBeans(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(RawRecordDao.class);
    service = new RawRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
  }

  @Override
  public RawRecordMocks getMocks() {
    return RawRecordMocks.mock();
  }

}