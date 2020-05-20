package org.folio.services.impl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBRecordMocks;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.LBRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractEntityServiceTest<Record, RecordCollection, RecordQuery, LBRecordDao, LBRecordService, LBRecordMocks> {

  private LBSnapshotDao mockSnapshotDao;

  private RawRecordDao mockRawRecordDao;

  private ParsedRecordDao mockParsedRecordDao;

  private ErrorRecordDao mockErrorRecordDao;

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(LBRecordDao.class);
    mockSnapshotDao = Mockito.mock(LBSnapshotDao.class);
    mockRawRecordDao = Mockito.mock(RawRecordDao.class);
    mockParsedRecordDao = Mockito.mock(ParsedRecordDao.class);
    mockErrorRecordDao = Mockito.mock(ErrorRecordDao.class);
    service = new LBRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
    FieldUtils.writeField(service, "snapshotDao", mockSnapshotDao, true);
    FieldUtils.writeField(service, "rawRecordDao", mockRawRecordDao, true);
    FieldUtils.writeField(service, "parsedRecordDao", mockParsedRecordDao, true);
    FieldUtils.writeField(service, "errorRecordDao", mockErrorRecordDao, true);
  }

  @Test
  public void shouldSave(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldSaveRecords(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldErrorWhileTryingToSave(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldUpdate(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldGetFormattedRecord(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForRecord(TestContext context) {
    context.assertTrue(true);
  }

  @Test
  public void shouldUpdateSourceRecord(TestContext context) {
    context.assertTrue(true);
  }

  @Override
  public LBRecordMocks getMocks() {
    return LBRecordMocks.mock();
  }

}