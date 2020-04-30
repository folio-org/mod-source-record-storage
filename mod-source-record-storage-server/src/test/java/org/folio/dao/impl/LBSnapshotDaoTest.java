package org.folio.dao.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.SnapshotFilter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.junit.runner.RunWith;

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
    return new Snapshot()
      .withJobExecutionId("ac8f351c-8a2e-11ea-bc55-0242ac130003")
      .withStatus(Snapshot.Status.NEW);
  }

  @Override
  public Snapshot getMockBeanWithoutId() {
    return new Snapshot().withStatus(Snapshot.Status.NEW);
  }

  @Override
  public Snapshot getInvalidMockBean() {
    return new Snapshot()
      .withJobExecutionId("f3ba7619-d9b6-4e7d-9ebf-587d2d3807d0");
  }

  @Override
  public Snapshot getUpdatedMockBean() {
    Date now = new Date();
    return getMockBean()
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
      .withProcessingStartedDate(now);
  }

  @Override
  public Snapshot[] getMockBeans() {
    return new Snapshot[] {
      new Snapshot()
        .withJobExecutionId("67dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.NEW),
      new Snapshot()
        .withJobExecutionId("17dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.NEW),
      new Snapshot()
        .withJobExecutionId("27dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
        .withProcessingStartedDate(new Date()),
      new Snapshot()
        .withJobExecutionId("37dfac11-1caf-4470-9ad1-d533f6360bdd")
        .withStatus(Snapshot.Status.PARSING_FINISHED)
        .withProcessingStartedDate(new Date()),
      new Snapshot()
        .withJobExecutionId("7644042a-805e-4465-b690-cafbc094d891")
        .withStatus(Snapshot.Status.DISCARDED)
        .withProcessingStartedDate(new Date())
    };
  }

  @Override
  public void compareBeans(Snapshot expected, Snapshot actual) {
    if (StringUtils.isEmpty(expected.getJobExecutionId())) {
      assertNotNull(actual.getJobExecutionId());
    } else {
      assertEquals(dao.getId(expected), dao.getId(actual));
    }
    assertEquals(expected.getStatus(), actual.getStatus());
    assertEquals(expected.getProcessingStartedDate(), actual.getProcessingStartedDate());
  }

  @Override
  public void assertTotal(Integer expected, SnapshotCollection actual) {
    assertEquals(expected, actual.getTotalRecords());
  }

  @Override
  public void assertNoopFilterResults(SnapshotCollection actual) {
    List<Snapshot> expected = Arrays.asList(getMockBeans());
    assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(SnapshotCollection actual) {
    List<Snapshot> expected = Arrays.asList(getMockBeans()).stream()
      .filter(bean -> bean.getStatus().equals(getArbitruaryFilter().getStatus()))
      .collect(Collectors.toList());
    assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

}