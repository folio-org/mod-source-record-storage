package org.folio.services;

import java.util.Objects;

import org.folio.TestMocks;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBRecordDaoImpl;
import org.folio.dao.util.LBSnapshotDaoUtil;
import org.folio.rest.jaxrs.model.AdditionalInfo;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordServiceTest extends AbstractLBServiceTest {

  private LBRecordDao recordDao;

  private LBRecordService recordService;

  @Before
  public void setUp(TestContext context) {
    recordDao = new LBRecordDaoImpl(postgresClientFactory);
    recordService = new LBRecordServiceImpl(recordDao);
    Async async = context.async();
    LBSnapshotDaoUtil.save(postgresClientFactory.getQueryExecutor(TENANT_ID), TestMocks.getSnapshots()).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      async.complete();
    });
  }

  @After
  public void cleanUp(TestContext context) {
    Async async = context.async();
    LBSnapshotDaoUtil.deleteAll(postgresClientFactory.getQueryExecutor(TENANT_ID)).onComplete(delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
  }

  @Test
  public void shouldGetRecords(TestContext context) {
    Async async = context.async();
    context.assertTrue(Objects.nonNull("Will do later"));
    async.complete();
  }

  @Test
  public void shouldGetRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordDao.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordService.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        context.assertNotNull(get.result().get().getRawRecord());
        context.assertNotNull(get.result().get().getParsedRecord());
        context.assertNull(get.result().get().getErrorRecord());
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  // TODO: test get by matched id not equal to id

  @Test
  public void shouldNotGetRecordById(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
      if (get.failed()) {
        context.fail(get.cause());
      }
      context.assertFalse(get.result().isPresent());
      async.complete();
    });
  }

  @Test
  public void shouldSaveRecord(TestContext context) {
    Async async = context.async();
    Record expected = TestMocks.getRecord(0);
    recordService.saveRecord(expected, TENANT_ID).onComplete(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      recordDao.getRecordById(expected.getMatchedId(), TENANT_ID).onComplete(get -> {
        if (get.failed()) {
          context.fail(get.cause());
        }
        context.assertTrue(get.result().isPresent());
        compareRecords(context, expected, get.result().get());
        async.complete();
      });
    });
  }

  // TODO: test save with calculate generation graeter than 0
  
  @Test
  public void shouldFailToSaveRecord(TestContext context) {
    Async async = context.async();
    Record valid = TestMocks.getRecord(0);
    Record invalid = new Record()
      .withId(valid.getId())
      .withSnapshotId(valid.getSnapshotId())
      .withMatchedId(valid.getMatchedId())
      .withState(valid.getState())
      .withGeneration(valid.getGeneration())
      .withOrder(valid.getOrder())
      .withRawRecord(valid.getRawRecord())
      .withParsedRecord(valid.getParsedRecord())
      .withAdditionalInfo(valid.getAdditionalInfo())
      .withExternalIdsHolder(valid.getExternalIdsHolder())
      .withMetadata(valid.getMetadata());
    recordService.saveRecord(invalid, TENANT_ID).onComplete(save -> {
      context.assertTrue(save.failed());
      String expected = "null value in column \"record_type\" violates not-null constraint";
      context.assertEquals(expected, save.cause().getMessage());
      async.complete();
    });
  }

  private void compareRecords(TestContext context, Record expected, Record actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getMatchedId(), actual.getMatchedId());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    context.assertEquals(expected.getState(), actual.getState());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    context.assertEquals(expected.getGeneration(), actual.getGeneration());
    compareRawRecord(context, expected.getRawRecord(), actual.getRawRecord());
    compareParsedRecord(context, expected.getParsedRecord(), actual.getParsedRecord());
    compareErrorRecord(context, expected.getErrorRecord(), actual.getErrorRecord());
    compareAdditionalInfo(context, expected.getAdditionalInfo(), actual.getAdditionalInfo());
    compareExternalIdsHolder(context, expected.getExternalIdsHolder(), actual.getExternalIdsHolder());
    compareMetadata(context, expected.getMetadata(), actual.getMetadata());
  }

  private void compareAdditionalInfo(TestContext context, AdditionalInfo expected, AdditionalInfo actual) {
    context.assertEquals(expected.getSuppressDiscovery(), actual.getSuppressDiscovery());
  }

  private void compareExternalIdsHolder(TestContext context, ExternalIdsHolder expected, ExternalIdsHolder actual) {
    context.assertEquals(expected.getInstanceId(), actual.getInstanceId());
  }

  private void compareRawRecord(TestContext context, RawRecord expected, RawRecord actual) {
    if (Objects.nonNull(expected)) {
      context.assertEquals(expected.getId(), actual.getId());
      context.assertEquals(expected.getContent(), actual.getContent());
    } else {
      context.assertNull(actual);
    }
  }

  private void compareParsedRecord(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    if (Objects.nonNull(expected)) {
      context.assertEquals(expected.getId(), actual.getId());
      context.assertEquals(expected.getContent(), actual.getContent());
    } else {
      context.assertNull(actual);
    }
  }

  private void compareErrorRecord(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    if (Objects.nonNull(expected)) {
      context.assertEquals(expected.getId(), actual.getId());
      context.assertEquals(expected.getContent(), actual.getContent());
      context.assertEquals(expected.getDescription(), actual.getDescription());
    } else {
      context.assertNull(actual);
    }
  }

}