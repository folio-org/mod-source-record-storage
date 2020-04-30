package org.folio.dao.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    CompositeFuture.all(
      snapshotDao.save(new Snapshot()
        .withJobExecutionId(UUID.randomUUID().toString())
        .withStatus(Snapshot.Status.PARSING_IN_PROGRESS), TENANT_ID),
      snapshotDao.save(new Snapshot()
        .withJobExecutionId(UUID.randomUUID().toString())
        .withStatus(Snapshot.Status.PARSING_IN_PROGRESS), TENANT_ID)
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
    String sql = String.format(DELETE_SQL_TEMPLATE, dao.getTableName());
    dao.getPostgresClient(TENANT_ID).execute(sql, delete -> {
      if (delete.failed()) {
        context.fail(delete.cause());
      }
      async.complete();
    });
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
    String id = "39c68762-bc37-45e0-b523-58b4ed63f32f";
    return new Record().withId(id)
      .withSnapshotId(snapshot1.getJobExecutionId())
      .withRecordType(Record.RecordType.MARC)
      .withMatchedId(id)
      .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
      .withOrder(0)
      .withGeneration(1)
      .withState(Record.State.ACTUAL);
  }

  @Override
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
    String id1 = "233e18c0-9fab-4f5e-aa35-376af1bdc63e";
    String id2 = "79a4c560-802d-4031-a7a0-ff338fe734f3";
    String id3 = "e028c34e-3f11-41a0-a481-09d0b7af4a17";
    String id4 = "5a9f334b-9612-471a-8730-ccf42995f682";
    String id5 = "09454964-b0a6-409c-a26e-07e0399f0e72";
    return new Record[] {
      new Record()
        .withId(id1)
        .withSnapshotId(snapshot1.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withMatchedId(id1)
        .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
        .withOrder(0)
        .withGeneration(1)
        .withState(Record.State.ACTUAL),
      new Record()
        .withId(id2)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withMatchedId(id2)
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withOrder(11)
        .withGeneration(1)
        .withState(Record.State.ACTUAL),
      new Record()
        .withId(id3)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withMatchedId(id3)
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withOrder(27)
        .withGeneration(2)
        .withState(Record.State.OLD),
      new Record()
        .withId(id4)
        .withSnapshotId(snapshot1.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withMatchedId(id4)
        .withMatchedProfileId("f9926e86-883b-4455-a807-fc5eeb9a951a")
        .withOrder(1)
        .withGeneration(1)
        .withState(Record.State.DRAFT),
      new Record()
        .withId(id5)
        .withSnapshotId(snapshot2.getJobExecutionId())
        .withRecordType(Record.RecordType.MARC)
        .withMatchedId(id5)
        .withMatchedProfileId("df7bf522-66e1-4b52-9d48-abd739f37934")
        .withOrder(101)
        .withGeneration(3)
        .withState(Record.State.ACTUAL)
    };
  }

  @Override
  public void compareBeans(Record expected, Record actual) {
    if (StringUtils.isEmpty(expected.getId())) {
      assertNotNull(actual.getId());
    } else {
      assertEquals(dao.getId(expected), dao.getId(actual));
    }

  }

  @Override
  public void assertTotal(Integer expected, RecordCollection actual) {
    assertEquals(expected, actual.getTotalRecords());
  }

  @Override
  public void assertNoopFilterResults(RecordCollection actual) {
    List<Record> expected = Arrays.asList(getMockBeans());
    assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(RecordCollection actual) {
    List<Record> expected = Arrays.asList(getMockBeans()).stream()
      .filter(bean -> bean.getState().equals(getArbitruaryFilter().getState()) &&
        bean.getMatchedProfileId().equals(getArbitruaryFilter().getMatchedProfileId()))
      .collect(Collectors.toList());
    assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRecord -> assertTrue(actual.getRecords().stream()
      .anyMatch(actualRecord -> actualRecord.getId().equals(expectedRecord.getId()))));
  }

}