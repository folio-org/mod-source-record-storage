package org.folio.dao.impl;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBRecordDao;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.RecordFilter;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.CompositeFuture;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBRecordDaoTest extends AbstractBeanDaoTest<Record, RecordCollection, RecordFilter, LBRecordDao> {

  private LBSnapshotDao snapshotDao;

  private Snapshot snapshot1;
  private Snapshot snapshot2;

  @Override
  public void createDependentBeans(TestContext context) {
    snapshotDao = new LBSnapshotDaoImpl(postgresClientFactory);
    Async async = context.async();

    Snapshot[] snapshots = MockSnapshotFactory.getMockSnapshots();
    CompositeFuture.all(
      snapshotDao.save(snapshots[0], TENANT_ID),
      snapshotDao.save(snapshots[1], TENANT_ID)
    ).setHandler(save -> {
      if (save.failed()) {
        context.fail(save.cause());
      }
      snapshot1 = save.result().resultAt(0);
      snapshot2 = save.result().resultAt(1);
      async.complete();
    });
  }

  @Override
  public void createDao(TestContext context) {
    dao = new LBRecordDaoImpl(postgresClientFactory);
  }

  @Override
  public void clearTables(TestContext context) {
    Async async = context.async();
    PostgresClient pgClient = PostgresClient.getInstance(vertx, TENANT_ID);
    String sql = String.format(DELETE_SQL_TEMPLATE, dao.getTableName());
    pgClient.execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      String snapshotSql = String.format(DELETE_SQL_TEMPLATE, snapshotDao.getTableName());
      pgClient.execute(snapshotSql, snapshotDelete -> {
        if (snapshotDelete.failed()) {
          context.fail(snapshotDelete.cause());
        }
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveGeneratingId(TestContext context) {
    Async async = context.async();
    dao.save(getMockBeanWithoutId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareBeans(context, getMockBeanWithoutId(), res.result());
      async.complete();
    });
  }

  public Record getMockBeanWithoutId() {
    return new Record()
      .withSnapshotId(snapshot2.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
      .withOrder(0)
      .withGeneration(1)
      .withState(Record.State.ACTUAL);
  }

  @Override
  public RecordFilter getNoopFilter() {
    return new RecordFilter();
  }

  @Override
  public RecordFilter getArbitruaryFilter() {
    RecordFilter snapshotFilter = new RecordFilter();
    snapshotFilter.setMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934");
    snapshotFilter.setState(Record.State.ACTUAL);
    return snapshotFilter;
  }

  @Override
  public Record getMockBean() {
    return MockRecordFactory.getMockRecord(snapshot1);
  }

  @Override
  public Record getInvalidMockBean() {
    String id = UUID.randomUUID().toString();
    return new Record().withId(id)
      .withRecordType(Record.RecordType.MARC)
      .withOrder(0)
      .withGeneration(1)
      .withState(Record.State.ACTUAL);
  }

  @Override
  public Record getUpdatedMockBean() {
    return getMockBean()
      .withState(Record.State.DRAFT)
      .withOrder(2);
  }

  @Override
  public Record[] getMockBeans() {
    return MockRecordFactory.getMockRecords(snapshot1, snapshot2);
  }

  @Override
  public void compareBeans(TestContext context, Record expected, Record actual) {
    if (StringUtils.isEmpty(expected.getId())) {
      context.assertNotNull(actual.getId());
    } else {
      context.assertEquals(expected.getId(), actual.getId());
    }
    if (StringUtils.isEmpty(expected.getMatchedId())) {
      context.assertNotNull(actual.getMatchedId());
    } else {
      context.assertEquals(expected.getMatchedId(), actual.getMatchedId());
    }
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getMatchedProfileId(), actual.getMatchedProfileId());
    context.assertEquals(expected.getGeneration(), actual.getGeneration());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    context.assertEquals(expected.getState(), actual.getState());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    if (expected.getAdditionalInfo() != null) {
      context.assertEquals(expected.getAdditionalInfo().getSuppressDiscovery(), actual.getAdditionalInfo().getSuppressDiscovery());
    }
    if (expected.getExternalIdsHolder() != null) {
      context.assertEquals(expected.getExternalIdsHolder().getInstanceId(), actual.getExternalIdsHolder().getInstanceId());
    }
    if (expected.getMetadata() != null) {
      context.assertEquals(expected.getMetadata().getCreatedByUserId(), actual.getMetadata().getCreatedByUserId());
      context.assertEquals(expected.getMetadata().getCreatedDate(), actual.getMetadata().getCreatedDate());
      context.assertEquals(expected.getMetadata().getUpdatedByUserId(), actual.getMetadata().getUpdatedByUserId());
      context.assertEquals(expected.getMetadata().getUpdatedDate(), actual.getMetadata().getUpdatedDate());
    }
  }

  @Override
  public void assertTotal(TestContext context, Integer expected, RecordCollection actual) {
    context.assertEquals(expected, actual.getTotalRecords());
  }

  @Override
  public void assertNoopFilterResults(TestContext context, RecordCollection actual) {
    List<Record> expected = Arrays.asList(getMockBeans());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> context.assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, RecordCollection actual) {
    List<Record> expected = Arrays.asList(getMockBeans()).stream()
      .filter(bean -> bean.getState().equals(getArbitruaryFilter().getState()) &&
        bean.getMatchedProfileId().equals(getArbitruaryFilter().getMatchedProfileId()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> context.assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

}