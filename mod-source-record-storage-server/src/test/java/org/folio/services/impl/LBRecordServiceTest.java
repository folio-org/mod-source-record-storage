package org.folio.services.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.LBRecordMocks;
import org.folio.dao.ErrorRecordDao;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.ExternalIdType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto;
import org.folio.rest.jaxrs.model.SuppressFromDiscoveryDto.IncomingIdType;
import org.folio.services.LBRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.vertx.core.Promise;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractEntityServiceTest<Record, RecordCollection, RecordQuery, LBRecordDao, LBRecordService, LBRecordMocks> {

  private LBSnapshotDao mockSnapshotDao;

  private RawRecordDao mockRawRecordDao;

  private ParsedRecordDao mockParsedRecordDao;

  private ErrorRecordDao mockErrorRecordDao;

  @Override
  public void createBeans(TestContext context) throws IllegalAccessException {
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

  // NOTE: tests mocking inTransaction are not a good measure of correctness
  // will have to use integration tests

  @Test
  public void shouldSave(TestContext context) {
    Promise<Record> saveRecord = Promise.promise();
    Record record = mocks.getMockEntity();
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(saveRecord.future());
    service.save(record, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      context.assertTrue(save.succeeded());
      mocks.compareEntities(context, record, save.result());
    });
    saveRecord.complete(record);
  }

  @Test
  public void shouldErrorWhileTryingToSave(TestContext context) {
    Promise<Record> saveRecord = Promise.promise();
    Record record = mocks.getMockEntity();
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(saveRecord.future());
    service.save(record, TENANT_ID).onComplete(save -> {
      context.assertTrue(save.failed());
    });
    saveRecord.fail("Failed");
  }

  @Test
  public void shouldSaveRecords(TestContext context) {
    List<Record> records = mocks.getMockEntities();
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(records)
      .withTotalRecords(records.size());

    List<Promise<Record>> promises = records.stream().map(record -> {
      Promise<Record> saveRecord = Promise.promise();
      saveRecord.complete(record);
      return saveRecord;
    }).collect(Collectors.toList());;

    final AtomicInteger index = new AtomicInteger(0);

    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenAnswer(new Answer() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return promises.get(index.getAndIncrement());
      }

    });

    service.saveRecords(recordCollection, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      context.assertEquals(records.size(), update.result().getTotalRecords());
      context.assertEquals(0, update.result().getErrorMessages().size());
      mocks.compareEntities(context, records, update.result().getRecords(), true);
    });
  }

  @Test
  public void shouldUpdate(TestContext context) {
    Promise<Record> updateRecord = Promise.promise();
    Record record = mocks.getMockEntity();
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(updateRecord.future());
    service.update(record, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      mocks.compareEntities(context, record, update.result());
    });
    updateRecord.complete(record);
  }

  @Test
  public void shouldErrorWithNotFoundWhileTryingToUpdate(TestContext context) {
    Promise<Record> updateRecord = Promise.promise();
    Record record = mocks.getMockEntity();
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(updateRecord.future());
    service.update(record, TENANT_ID).onComplete(update -> {
      context.assertTrue(update.failed());
    });
    updateRecord.fail("Failed");
  }

  @Test
  public void shouldGetFormattedRecord(TestContext context) {
    Promise<Optional<Record>> recordPromise = Promise.promise();
    Promise<Optional<ParsedRecord>> parsedRecordPromise = Promise.promise();
    Record record = mocks.getMockEntity();
    ParsedRecord parsedRecord = record.getParsedRecord();
    when(mockDao.getRecordByExternalId(eq(record.getId()), eq(ExternalIdType.INSTANCE), eq(TENANT_ID))).thenReturn(recordPromise.future());
    when(mockParsedRecordDao.getById(eq(record.getId()),  eq(TENANT_ID))).thenReturn(parsedRecordPromise.future());
    service.getFormattedRecord(ExternalIdType.INSTANCE.toString(), record.getId(), TENANT_ID).onComplete(fr -> {
      if (fr.failed()) {
        context.fail(fr.cause());
      }
      context.assertTrue(fr.succeeded());
      context.assertEquals(record.getParsedRecord().getFormattedContent(), fr.result().getParsedRecord().getFormattedContent());
    });
    recordPromise.complete(Optional.of(record));
    parsedRecordPromise.complete(Optional.of(parsedRecord));
  }

  @Test
  public void shouldUpdateSuppressFromDiscoveryForRecord(TestContext context) {
    Promise<Record> recordPromise = Promise.promise();
    Record record = mocks.getMockEntity();
    SuppressFromDiscoveryDto suppressFromDiscoveryDto = new SuppressFromDiscoveryDto()
      .withId(record.getExternalIdsHolder().getInstanceId())
      .withIncomingIdType(IncomingIdType.INSTANCE)
      .withSuppressFromDiscovery(true);
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(recordPromise.future());
    service.updateSuppressFromDiscoveryForRecord(suppressFromDiscoveryDto, TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      mocks.compareEntities(context, record.withAdditionalInfo(record.getAdditionalInfo().withSuppressDiscovery(true)), update.result());
    });
    recordPromise.complete(record);
  }

  @Test
  public void shouldUpdateSourceRecord(TestContext context) {
    Promise<Record> recordPromise = Promise.promise();
    Record record = mocks.getMockEntity();
    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(record.getId())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(record.getRecordType().toString()))
      .withParsedRecord(record.getParsedRecord())
      .withExternalIdsHolder(record.getExternalIdsHolder())
      .withAdditionalInfo(record.getAdditionalInfo())
      .withMetadata(record.getMetadata());
    when(mockDao.inTransaction(eq(TENANT_ID), any(Function.class))).thenReturn(recordPromise.future());
    service.updateSourceRecord(parsedRecordDto, "a16de3f5-5751-4d8f-82a4-0b9f21238c51", TENANT_ID).onComplete(update -> {
      if (update.failed()) {
        context.fail(update.cause());
      }
      context.assertTrue(update.succeeded());
      mocks.compareEntities(context, record, update.result());
    });
    recordPromise.complete(record);
  }

  @Override
  public LBRecordMocks getMocks() {
    return LBRecordMocks.mock();
  }

}