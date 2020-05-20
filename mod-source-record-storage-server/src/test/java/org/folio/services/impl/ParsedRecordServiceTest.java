package org.folio.services.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.ParsedRecordMocks;
import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.services.ParsedRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.vertx.core.Promise;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordServiceTest extends AbstractEntityServiceTest<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery, ParsedRecordDao, ParsedRecordService, ParsedRecordMocks> {

  LBRecordDao mockRecordDao;

  @Override
  public void createBeans(TestContext context) throws IllegalAccessException {
    mockDao = Mockito.mock(ParsedRecordDao.class);
    mockRecordDao = Mockito.mock(LBRecordDao.class);
    service = new ParsedRecordServiceImpl();
    FieldUtils.writeField(service, "dao", mockDao, true);
    FieldUtils.writeField(service, "recordDao", mockRecordDao, true);
  }

  // NOTE: tests mocking inTransaction are not a good measure of correctness
  // will have to use integration tests

  @Test
  public void shouldUpdateParsedRecord(TestContext context) {
    Promise<ParsedRecord> saveParsedRecord = Promise.promise();
    Record record = TestMocks.getRecord(0);
    ParsedRecord parsedRecord = record.getParsedRecord();
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(saveParsedRecord.future());
    service.updateParsedRecord(record, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      mocks.compareEntities(context, parsedRecord, update.result());
    });
    saveParsedRecord.complete(parsedRecord);
  }

  @Test
  public void shouldUpdateParsedRecords(TestContext context) {
    List<Record> records = TestMocks.getRecords();
    List<ParsedRecord> parsedRecords = records.stream()
          .map(record -> record.getParsedRecord())
          .collect(Collectors.toList());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());

    List<Promise<ParsedRecord>> promises = parsedRecords.stream().map(parsedRecord -> {
      Promise<ParsedRecord> saveParsedRecord = Promise.promise();
      saveParsedRecord.complete(parsedRecord);
      return saveParsedRecord;
    }).collect(Collectors.toList());;

    final AtomicInteger index = new AtomicInteger(0);

    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenAnswer(new Answer() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return promises.get(index.getAndIncrement());
      }

    });

    service.updateParsedRecords(recordCollection, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      context.assertEquals(parsedRecords.size(), update.result().getTotalRecords());
      context.assertEquals(0, update.result().getErrorMessages().size());
      mocks.compareEntities(context, parsedRecords, update.result().getParsedRecords(), true);
    });
  }

  @Override
  public ParsedRecordMocks getMocks() {
    return ParsedRecordMocks.mock();
  }

}