package org.folio.services.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.ErrorRecordMocks;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.query.ErrorRecordQuery;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.folio.services.ErrorRecordService;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ErrorRecordServiceTest extends AbstractEntityServiceTest<ErrorRecord, ErrorRecordCollection, ErrorRecordQuery, ErrorRecordDao, ErrorRecordService, ErrorRecordMocks> {

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(ErrorRecordDao.class);
    service = new ErrorRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
  }

  @Override
  public ErrorRecordMocks getMocks() {
    return ErrorRecordMocks.mock();
  }

}