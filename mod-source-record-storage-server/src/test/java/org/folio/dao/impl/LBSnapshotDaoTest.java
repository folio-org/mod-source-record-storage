package org.folio.dao.impl;

import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.SnapshotFilter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanUtils;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class LBSnapshotDaoTest extends AbstractEntityDaoTest<Snapshot, SnapshotCollection, SnapshotFilter, LBSnapshotDao> {

  @Override
  public void createDao(TestContext context) throws IllegalAccessException {
    dao = new LBSnapshotDaoImpl();
    FieldUtils.writeField(dao, "postgresClientFactory", postgresClientFactory, true);
  }

  @Override
  public void createDependentEntities(TestContext context) {
    // NOTE: no dependent entities needed for testing Snapshot DAO
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
    dao.save(getMockEntityWithoutId(), TENANT_ID).setHandler(res -> {
      if (res.failed()) {
        context.fail(res.cause());
      }
      compareEntities(context, getMockEntityWithoutId(), res.result());
      async.complete();
    });
  }

  public Snapshot getMockEntityWithoutId() {
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
  public Snapshot getMockEntity() {
    return getSnapshot(0);
  }

  @Override
  public Snapshot getInvalidMockEntity() {
    return new Snapshot()
      .withJobExecutionId("f3ba7619-d9b6-4e7d-9ebf-587d2d3807d0");
  }

  @Override
  public Snapshot getUpdatedMockEntity() {
    return new Snapshot()
      .withJobExecutionId(getMockEntity().getJobExecutionId())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
      .withProcessingStartedDate(new Date());
  }

  @Override
  public List<Snapshot> getMockEntities() {
    return getSnapshots();
  }

  @Override
  public void compareEntities(TestContext context, Snapshot expected, Snapshot actual) {
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
    List<Snapshot> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, SnapshotCollection actual) {
    List<Snapshot> expected = getMockEntities().stream()
      .filter(entity -> entity.getStatus().equals(getArbitruaryFilter().getStatus()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  @Override
  public SnapshotFilter getCompleteFilter() {
    SnapshotFilter filter = new SnapshotFilter();
    BeanUtils.copyProperties(getSnapshot("6681ef31-03fe-4abc-9596-23de06d575c5").get(), filter);
    filter.withProcessingStartedDate(null);
    return filter;
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = '6681ef31-03fe-4abc-9596-23de06d575c5'" +
      " AND status = 'PROCESSING_IN_PROGRESS'";
  }

}