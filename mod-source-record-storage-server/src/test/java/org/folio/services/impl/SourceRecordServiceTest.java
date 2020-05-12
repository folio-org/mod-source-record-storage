package org.folio.services.impl;

import static org.folio.SourceRecordTestHelper.compareSourceRecord;
import static org.folio.SourceRecordTestHelper.compareSourceRecordCollection;
import static org.folio.SourceRecordTestHelper.enhanceWithParsedRecord;
import static org.folio.SourceRecordTestHelper.enhanceWithRawRecord;
import static org.folio.SourceRecordTestHelper.getParsedRecords;
import static org.folio.SourceRecordTestHelper.getRawRecords;
import static org.folio.SourceRecordTestHelper.getRecords;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.ParsedRecordDao;
import org.folio.dao.RawRecordDao;
import org.folio.dao.SourceRecordDao;
import org.folio.dao.query.RecordQuery;
import org.folio.dao.util.SourceRecordContent;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.State;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordCollection;
import org.folio.services.SourceRecordService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class SourceRecordServiceTest extends AbstractServiceTest {

  private SourceRecordService sourceRecordService;

  private SourceRecordDao mockSourceRecordDao;

  private LBRecordDao mockRecordDao;

  private RawRecordDao mockRawRecordDao;

  private ParsedRecordDao mockParsedRecordDao;

  @Override
  public void createService(TestContext context) throws IllegalAccessException {
    mockSourceRecordDao = Mockito.mock(SourceRecordDao.class);
    mockRecordDao = Mockito.mock(LBRecordDao.class);
    mockRawRecordDao = Mockito.mock(RawRecordDao.class);
    mockParsedRecordDao = Mockito.mock(ParsedRecordDao.class);
    sourceRecordService = new SourceRecordServiceImpl();
    FieldUtils.writeField(sourceRecordService, "sourceRecordDao", mockSourceRecordDao, true);
    FieldUtils.writeField(sourceRecordService, "recordDao", mockRecordDao, true);
    FieldUtils.writeField(sourceRecordService, "rawRecordDao", mockRawRecordDao, true);
    FieldUtils.writeField(sourceRecordService, "parsedRecordDao", mockParsedRecordDao, true);
  }

  @Test
  public void shouldGetSourceMarcRecordById(TestContext context) {
    Promise<Optional<SourceRecord>> getByIdPromise = Promise.promise();
    Record expectedRecord = TestMocks.getRecord(0);
    ParsedRecord expectedParsedRecord = TestMocks.getParsedRecord(0);
    when(mockSourceRecordDao.getSourceMarcRecordById(expectedRecord.getId(), TENANT_ID)).thenReturn(getByIdPromise.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordById(expectedRecord.getId(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, expectedRecord, expectedParsedRecord, res.result());
      async.complete();
    });
    SourceRecord actualSourceRecord = new SourceRecord()
      .withRecordId(expectedRecord.getId())
      .withParsedRecord(expectedParsedRecord);
    getByIdPromise.complete(Optional.of(actualSourceRecord));
  }

  @Test
  public void shouldGetSourceMarcRecordByIdAlt(TestContext context) {
    Promise<Optional<SourceRecord>> getByIdAltPromise = Promise.promise();
    Record expectedRecord = TestMocks.getRecord(0);
    ParsedRecord expectedParsedRecord = TestMocks.getParsedRecord(0);
    when(mockSourceRecordDao.getSourceMarcRecordByIdAlt(expectedRecord.getId(), TENANT_ID)).thenReturn(getByIdAltPromise.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordByIdAlt(expectedRecord.getId(), TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected record and parsed record should be updated
      compareSourceRecord(context, expectedRecord, expectedParsedRecord, res.result());
      async.complete();
    });
    SourceRecord actualSourceRecord = new SourceRecord()
      .withRecordId(expectedRecord.getId())
      .withParsedRecord(expectedParsedRecord);
      getByIdAltPromise.complete(Optional.of(actualSourceRecord));
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceId(TestContext context) {
    Promise<Optional<SourceRecord>> getByInstanceIdPromise = Promise.promise();
    Record expectedRecord = TestMocks.getRecord(0);
    ParsedRecord expectedParsedRecord = TestMocks.getParsedRecord(0);
    String instanceId = expectedRecord.getExternalIdsHolder().getInstanceId();
    when(mockSourceRecordDao.getSourceMarcRecordByInstanceId(instanceId, TENANT_ID)).thenReturn(getByInstanceIdPromise.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordByInstanceId(instanceId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, expectedRecord, expectedParsedRecord, res.result());
      async.complete();
    });
    SourceRecord actualSourceRecord = new SourceRecord()
      .withRecordId(expectedRecord.getId())
      .withParsedRecord(expectedParsedRecord);
      getByInstanceIdPromise.complete(Optional.of(actualSourceRecord));
  }

  @Test
  public void shouldGetSourceMarcRecordByInstanceIdAlt(TestContext context) {
    Promise<Optional<SourceRecord>> getByInstanceIdAltPromise = Promise.promise();
    Record expectedRecord = TestMocks.getRecord(0);
    ParsedRecord expectedParsedRecord = TestMocks.getParsedRecord(0);
    String instanceId = expectedRecord.getExternalIdsHolder().getInstanceId();
    when(mockSourceRecordDao.getSourceMarcRecordByInstanceIdAlt(instanceId, TENANT_ID)).thenReturn(getByInstanceIdAltPromise.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordByInstanceIdAlt(instanceId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      // NOTE: some new mock data should be introduced to ensure assertion of latest generation
      // when done the expected record and parsed record should be updated
      compareSourceRecord(context, expectedRecord, expectedParsedRecord, res.result());
      async.complete();
    });
    SourceRecord actualSourceRecord = new SourceRecord()
      .withRecordId(expectedRecord.getId())
      .withParsedRecord(expectedParsedRecord);
      getByInstanceIdAltPromise.complete(Optional.of(actualSourceRecord));
  }

  @Test
  public void shouldGetSourceMarcRecords(TestContext context) {
    Promise<SourceRecordCollection> getSourceMarcRecords = Promise.promise();
    List<Record> expectedRecords = getRecords(State.ACTUAL);
    List<RawRecord> expectedRawRecords = new ArrayList<>();
    List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
    when(mockSourceRecordDao.getSourceMarcRecords(0, 10, TENANT_ID)).thenReturn(getSourceMarcRecords.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecords(0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
    getSourceMarcRecords.complete(toSourceRecordCollection(expectedRecords, expectedRawRecords, expectedParsedRecords));
  }

  @Test
  public void shouldGetSourceMarcRecordsAlt(TestContext context) {
    Promise<SourceRecordCollection> getSourceMarcRecords = Promise.promise();
    // NOTE: some new mock data should be introduced to ensure assertion of latest generation
    // when done the expected records and parsed records will have to be manually filtered
    List<Record> expectedRecords = getRecords(State.ACTUAL);
    List<RawRecord> expectedRawRecords = new ArrayList<>();
    List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
    when(mockSourceRecordDao.getSourceMarcRecordsAlt(0, 10, TENANT_ID)).thenReturn(getSourceMarcRecords.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordsAlt(0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
    getSourceMarcRecords.complete(toSourceRecordCollection(expectedRecords, expectedRawRecords, expectedParsedRecords));
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriod(TestContext context) throws ParseException {
    Promise<SourceRecordCollection> getSourceMarcRecords = Promise.promise();
    List<Record> expectedRecords = getRecords(State.ACTUAL);
    List<RawRecord> expectedRawRecords = new ArrayList<>();
    List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    DateUtils.addHours(new Date(), 1);
    when(mockSourceRecordDao.getSourceMarcRecordsForPeriod(from, till, 0, 10, TENANT_ID)).thenReturn(getSourceMarcRecords.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordsForPeriod(from, till, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
    getSourceMarcRecords.complete(toSourceRecordCollection(expectedRecords, expectedRawRecords, expectedParsedRecords));
  }

  @Test
  public void shouldGetSourceMarcRecordsForPeriodAlt(TestContext context) throws ParseException {
    Promise<SourceRecordCollection> getSourceMarcRecords = Promise.promise();
    // NOTE: some new mock data should be introduced to ensure assertion of latest generation
    // when done the expected records and parsed records will have to be manually filtered
    List<Record> expectedRecords = getRecords(State.ACTUAL);
    List<RawRecord> expectedRawRecords = new ArrayList<>();
    List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZ");
    Date from = dateFormat.parse("2020-03-01T12:00:00-0500");
    Date till = DateUtils.addHours(new Date(), 1);
    when(mockSourceRecordDao.getSourceMarcRecordsForPeriodAlt(from, till, 0, 10, TENANT_ID)).thenReturn(getSourceMarcRecords.future());
    Async async = context.async();
    sourceRecordService.getSourceMarcRecordsForPeriodAlt(from, till, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });
    getSourceMarcRecords.complete(toSourceRecordCollection(expectedRecords, expectedRawRecords, expectedParsedRecords));
  }

  @Test
  public void shouldGetSourceMarcRecordWithContentById(TestContext context) {
    Promise<Optional<Record>> getRecordByIdPromise = Promise.promise();
    Promise<Optional<RawRecord>> getRawRecordByIdPromise = Promise.promise();
    Promise<Optional<ParsedRecord>> getParsedRecordByIdPromise = Promise.promise();
    Async async = context.async();
    SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;    
    Record expectedRecord = TestMocks.getRecord(0);
    Optional<RawRecord> expectedRawRecord = TestMocks.getRawRecord(expectedRecord.getId());
    assertTrue(expectedRawRecord.isPresent());
    Optional<ParsedRecord> expectedParsedRecord = TestMocks.getParsedRecord(expectedRecord.getId());
    assertTrue(expectedParsedRecord.isPresent());
    String id = expectedRecord.getId();
    when(mockRecordDao.getById(id, TENANT_ID)).thenReturn(getRecordByIdPromise.future());
    when(mockRawRecordDao.getById(id, TENANT_ID)).thenReturn(getRawRecordByIdPromise.future());
    when(mockParsedRecordDao.getById(id, TENANT_ID)).thenReturn(getParsedRecordByIdPromise.future());
    sourceRecordService.getSourceMarcRecordById(content, id, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, expectedRecord, expectedParsedRecord.get(), res.result());
      context.assertEquals(expectedRawRecord.get().getId(), res.result().get().getRawRecord().getId());
      context.assertEquals(expectedRawRecord.get().getContent(), res.result().get().getRawRecord().getContent());
      async.complete();
    });
    getRecordByIdPromise.complete(Optional.of(expectedRecord));
    getRawRecordByIdPromise.complete(expectedRawRecord);
    getParsedRecordByIdPromise.complete(expectedParsedRecord);
  }

  @Test
  public void shouldGetSourceMarcRecordWithContentByMatchedId(TestContext context) {
    Promise<Optional<Record>> getRecordByIdPromise = Promise.promise();
    Promise<Optional<RawRecord>> getRawRecordByIdPromise = Promise.promise();
    Promise<Optional<ParsedRecord>> getParsedRecordByIdPromise = Promise.promise();
    Async async = context.async();
    SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;    
    Record expectedRecord = TestMocks.getRecord(0);
    Optional<RawRecord> expectedRawRecord = TestMocks.getRawRecord(expectedRecord.getMatchedId());
    assertTrue(expectedRawRecord.isPresent());
    Optional<ParsedRecord> expectedParsedRecord = TestMocks.getParsedRecord(expectedRecord.getMatchedId());
    assertTrue(expectedParsedRecord.isPresent());
    String matchedId = expectedRecord.getMatchedId();
    when(mockRecordDao.getByMatchedId(matchedId, TENANT_ID)).thenReturn(getRecordByIdPromise.future());
    when(mockRawRecordDao.getById(matchedId, TENANT_ID)).thenReturn(getRawRecordByIdPromise.future());
    when(mockParsedRecordDao.getById(matchedId, TENANT_ID)).thenReturn(getParsedRecordByIdPromise.future());
    sourceRecordService.getSourceMarcRecordByMatchedId(content, matchedId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, expectedRecord, expectedParsedRecord.get(), res.result());
      context.assertEquals(expectedRawRecord.get().getId(), res.result().get().getRawRecord().getId());
      context.assertEquals(expectedRawRecord.get().getContent(), res.result().get().getRawRecord().getContent());
      async.complete();
    });
    getRecordByIdPromise.complete(Optional.of(expectedRecord));
    getRawRecordByIdPromise.complete(expectedRawRecord);
    getParsedRecordByIdPromise.complete(expectedParsedRecord);
  }

  @Test
  public void shouldGetSourceMarcRecordWithContentByInstanceId(TestContext context) {
    Promise<Optional<Record>> getRecordByIdPromise = Promise.promise();
    Promise<Optional<RawRecord>> getRawRecordByIdPromise = Promise.promise();
    Promise<Optional<ParsedRecord>> getParsedRecordByIdPromise = Promise.promise();
    Async async = context.async();
    SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;    
    Record expectedRecord = TestMocks.getRecord(0);
    Optional<RawRecord> expectedRawRecord = TestMocks.getRawRecord(expectedRecord.getId());
    assertTrue(expectedRawRecord.isPresent());
    Optional<ParsedRecord> expectedParsedRecord = TestMocks.getParsedRecord(expectedRecord.getId());
    assertTrue(expectedParsedRecord.isPresent());
    String instanceId = expectedRecord.getExternalIdsHolder().getInstanceId();
    when(mockRecordDao.getByInstanceId(instanceId, TENANT_ID)).thenReturn(getRecordByIdPromise.future());
    when(mockRawRecordDao.getById(expectedRecord.getId(), TENANT_ID)).thenReturn(getRawRecordByIdPromise.future());
    when(mockParsedRecordDao.getById(expectedRecord.getId(), TENANT_ID)).thenReturn(getParsedRecordByIdPromise.future());
    
    sourceRecordService.getSourceMarcRecordByInstanceId(content, instanceId, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecord(context, expectedRecord, expectedParsedRecord.get(), res.result());
      context.assertEquals(expectedRawRecord.get().getId(), res.result().get().getRawRecord().getId());
      context.assertEquals(expectedRawRecord.get().getContent(), res.result().get().getRawRecord().getContent());
      async.complete();
    });
    getRecordByIdPromise.complete(Optional.of(expectedRecord));
    getRawRecordByIdPromise.complete(expectedRawRecord);
    getParsedRecordByIdPromise.complete(expectedParsedRecord);
  }

  @Test
  public void shouldGetSourceMarcRecordsByQuery(TestContext context) {
    Promise<RecordCollection> getRecordsByQueryPromise = Promise.promise();

    List<Record> expectedRecords = TestMocks.getRecords();
    List<RawRecord> expectedRawRecords = getRawRecords(expectedRecords);
    List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);

    List<Promise<Optional<RawRecord>>> getRawRecordByIdPromises = expectedRawRecords.stream().map(rr -> {
      Promise<Optional<RawRecord>> getRawRecordByIdPromise = Promise.promise(); 
      when(mockRawRecordDao.getById(rr.getId(), TENANT_ID)).thenReturn(getRawRecordByIdPromise.future());
      return getRawRecordByIdPromise;
    }).collect(Collectors.toList());

    List<Promise<Optional<ParsedRecord>>> getParsedRecordByIdPromises = expectedParsedRecords.stream().map(rr -> {
      Promise<Optional<ParsedRecord>> getParsedRecordByIdPromise = Promise.promise(); 
      when(mockParsedRecordDao.getById(rr.getId(), TENANT_ID)).thenReturn(getParsedRecordByIdPromise.future());
      return getParsedRecordByIdPromise;
    }).collect(Collectors.toList());

    Async async = context.async();
    SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;
    RecordQuery query = new RecordQuery();

    when(mockRecordDao.getByQuery(query, 0, 10, TENANT_ID)).thenReturn(getRecordsByQueryPromise.future());

    sourceRecordService.getSourceMarcRecordsByQuery(content, query, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });

    RecordCollection expectedRecordCollection = new RecordCollection()
      .withRecords(expectedRecords)
      .withTotalRecords(expectedRecords.size());
    getRecordsByQueryPromise.complete(expectedRecordCollection);

    for (int i = 0; i < getRawRecordByIdPromises.size(); i++) {
      getRawRecordByIdPromises.get(i).complete(Optional.of(expectedRawRecords.get(i)));
    }
    for (int i = 0; i < getParsedRecordByIdPromises.size(); i++) {
      getParsedRecordByIdPromises.get(i).complete(Optional.of(expectedParsedRecords.get(i)));
    }
  }

  @Test
  public void shouldGetSourceMarcRecordsByQuerySorted(TestContext context) {
    Promise<RecordCollection> getRecordsByQueryPromise = Promise.promise();

    List<Record> expectedRecords = TestMocks.getRecords();
    Collections.sort(expectedRecords, (r1, r2) -> r1.getId().compareTo(r2.getId()));

    List<RawRecord> expectedRawRecords = getRawRecords(expectedRecords);
    List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);

    List<Promise<Optional<RawRecord>>> getRawRecordByIdPromises = expectedRawRecords.stream().map(rr -> {
      Promise<Optional<RawRecord>> getRawRecordByIdPromise = Promise.promise(); 
      when(mockRawRecordDao.getById(rr.getId(), TENANT_ID)).thenReturn(getRawRecordByIdPromise.future());
      return getRawRecordByIdPromise;
    }).collect(Collectors.toList());

    List<Promise<Optional<ParsedRecord>>> getParsedRecordByIdPromises = expectedParsedRecords.stream().map(rr -> {
      Promise<Optional<ParsedRecord>> getParsedRecordByIdPromise = Promise.promise(); 
      when(mockParsedRecordDao.getById(rr.getId(), TENANT_ID)).thenReturn(getParsedRecordByIdPromise.future());
      return getParsedRecordByIdPromise;
    }).collect(Collectors.toList());

    Async async = context.async();
    SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;
    RecordQuery query = (RecordQuery) new RecordQuery().orderBy("id");

    when(mockRecordDao.getByQuery(query, 0, 10, TENANT_ID)).thenReturn(getRecordsByQueryPromise.future());

    sourceRecordService.getSourceMarcRecordsByQuery(content, query, 0, 10, TENANT_ID).onComplete(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareSourceRecordCollection(context, expectedRecords, expectedRawRecords, expectedParsedRecords, res.result());
      async.complete();
    });

    RecordCollection expectedRecordCollection = new RecordCollection()
      .withRecords(expectedRecords)
      .withTotalRecords(expectedRecords.size());
    getRecordsByQueryPromise.complete(expectedRecordCollection);

    for (int i = 0; i < getRawRecordByIdPromises.size(); i++) {
      getRawRecordByIdPromises.get(i).complete(Optional.of(expectedRawRecords.get(i)));
    }
    for (int i = 0; i < getParsedRecordByIdPromises.size(); i++) {
      getParsedRecordByIdPromises.get(i).complete(Optional.of(expectedParsedRecords.get(i)));
    }
  }

  // @Test
  // public void shouldStreamGetSourceMarcRecordsByQuery(TestContext context) {
  //   Async async = context.async();
  //   SourceRecordContent content = SourceRecordContent.RAW_AND_PARSED_RECORD;
  //   RecordQuery query = new RecordQuery();
  //   List<SourceRecord> actualSourceRecords = new ArrayList<>();
  //   sourceRecordDao.getSourceMarcRecordsByQuery(content, query, 0, 10, TENANT_ID, sourceRecord -> {
  //     actualSourceRecords.add(sourceRecord);
  //   }, finished -> {
  //     if (finished.failed()) {
  //       context.fail(finished.cause());
  //     }
  //     List<Record> expectedRecords = getRecords();
  //     List<RawRecord> expectedRawRecords = getRawRecords(expectedRecords);
  //     List<ParsedRecord> expectedParsedRecords = getParsedRecords(expectedRecords);
  //     compareSourceRecords(context, expectedRecords, expectedRawRecords, expectedParsedRecords, actualSourceRecords);
  //     async.complete();
  //   });
  // }

  private SourceRecordCollection toSourceRecordCollection(List<Record> expectedRecords, List<RawRecord> expectedRawRecords,
      List<ParsedRecord> expectedParsedRecords) {
    List<SourceRecord> sourceRecords = expectedRecords.stream()
      .map(r -> sourceRecordService.toSourceRecord(r))
      .map(sr -> enhanceWithRawRecord(sr, expectedRawRecords))
      .map(sr -> enhanceWithParsedRecord(sr, expectedParsedRecords))
      .collect(Collectors.toList());
    return new SourceRecordCollection()
      .withSourceRecords(sourceRecords)
      .withTotalRecords(expectedRecords.size());
  }

}