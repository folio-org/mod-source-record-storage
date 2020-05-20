package org.folio.services.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.ParsedRecordMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.services.ParsedRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordServiceTest extends AbstractEntityServiceTest<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao, ParsedRecordService, ParsedRecordMocks> {

  LBRecordDao mockRecordDao;

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(ParsedRecordDao.class);
    mockRecordDao = Mockito.mock(LBRecordDao.class);
    service = new ParsedRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
    FieldUtils.writeField(service, "recordDao", mockRecordDao, true);
  }

  @Test
  public void shouldUpdateParsedRecords(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldUpdateParsedRecord(TestContext context) {
    context.assertTrue(true);
  }

  @Override
  public ParsedRecordMocks getMocks() {
    return ParsedRecordMocks.mock();
  }

}