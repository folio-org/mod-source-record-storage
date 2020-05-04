package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.SnapshotFilter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBSnapshotDaoTest extends AbstractBeanDaoTest<Snapshot, SnapshotCollection, SnapshotFilter, LBSnapshotDao> {

  @Override
  public void createDependentBeans(TestContext context) {
    // NOTE: no dependent beans needed for testing Snapshot DAO
  }

  @Override
  public void createDao(TestContext context) {
    dao = new LBSnapshotDaoImpl(postgresClientFactory);
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
      async.complete();
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

  public Snapshot getMockBeanWithoutId() {
    return new Snapshot()
      .withStatus(Snapshot.Status.NEW);
  }

  @Override
  public SnapshotFilter getNoopFilter() {
    return new SnapshotFilter();
  }

  @Override
  public SnapshotFilter getArbitruaryFilter() {
    SnapshotFilter snapshotFilter = new SnapshotFilter();
    snapshotFilter.setStatus(Snapshot.Status.NEW);
    return snapshotFilter;
  }

  @Override
  public Snapshot getMockBean() {
    return getSnapshot(0);
  }

  @Override
  public Snapshot getInvalidMockBean() {
    return new Snapshot()
      .withJobExecutionId("f3ba7619-d9b6-4e7d-9ebf-587d2d3807d0");
  }

  @Override
  public Snapshot getUpdatedMockBean() {
    return new Snapshot()
      .withJobExecutionId(getMockBean().getJobExecutionId())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
      .withProcessingStartedDate(new Date());
  }

  @Override
  public List<Snapshot> getMockBeans() {
    return getSnapshots();
  }

  @Override
  public void compareBeans(TestContext context, Snapshot expected, Snapshot actual) {
    if (StringUtils.isEmpty(expected.getJobExecutionId())) {
      context.assertNotNull(actual.getJobExecutionId());
    } else {
      context.assertEquals(expected.getJobExecutionId(), actual.getJobExecutionId());
    }
    context.assertEquals(expected.getStatus(), actual.getStatus());
    if (expected.getProcessingStartedDate() != null) {
      context.assertEquals(DATE_FORMATTER.format(expected.getProcessingStartedDate()), 
        DATE_FORMATTER.format(actual.getProcessingStartedDate().getTime()));
    }
  }

  @Override
  public void assertNoopFilterResults(TestContext context, SnapshotCollection actual) {
    List<Snapshot> expected = getMockBeans();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, SnapshotCollection actual) {
    List<Snapshot> expected = getMockBeans().stream()
      .filter(bean -> bean.getStatus().equals(getArbitruaryFilter().getStatus()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

}