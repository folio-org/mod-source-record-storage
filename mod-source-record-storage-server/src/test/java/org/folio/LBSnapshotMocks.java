package org.folio;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.springframework.beans.BeanUtils;

import io.vertx.ext.unit.TestContext;

public class LBSnapshotMocks implements EntityMocks<Snapshot, SnapshotCollection, SnapshotQuery> {

  private LBSnapshotMocks() { }

  public String getId(Snapshot snapshot) {
    return snapshot.getJobExecutionId();
  }

  public SnapshotQuery getNoopQuery() {
    return new SnapshotQuery();
  }

  public SnapshotQuery getArbitruaryQuery() {
    SnapshotQuery snapshotQuery = new SnapshotQuery();
    snapshotQuery.setStatus(Snapshot.Status.NEW);
    return snapshotQuery;
  }

  public SnapshotQuery getArbitruarySortedQuery() {
    return (SnapshotQuery) getArbitruaryQuery()
      .orderBy("status");
  }

  public SnapshotQuery getCompleteQuery() {
    SnapshotQuery query = new SnapshotQuery();
    BeanUtils.copyProperties(TestMocks.getSnapshot("6681ef31-03fe-4abc-9596-23de06d575c5").get(), query);
    query.withProcessingStartedDate(null);
    return query;
  }

  public Snapshot getMockEntity() {
    return TestMocks.getSnapshot(0);
  }

  public Snapshot getInvalidMockEntity() {
    return new Snapshot()
      .withJobExecutionId("f3ba7619-d9b6-4e7d-9ebf-587d2d3807d0");
  }

  public Snapshot getUpdatedMockEntity() {
    return new Snapshot()
      .withJobExecutionId(getMockEntity().getJobExecutionId())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
      .withProcessingStartedDate(new Date());
  }

  public List<Snapshot> getMockEntities() {
    return TestMocks.getSnapshots();
  }

  public void compareEntities(TestContext context, Snapshot expected, Snapshot actual) {
    if (StringUtils.isEmpty(expected.getJobExecutionId())) {
      context.assertNotNull(actual.getJobExecutionId());
    } else {
      context.assertEquals(expected.getJobExecutionId(), actual.getJobExecutionId());
    }
    context.assertEquals(expected.getStatus(), actual.getStatus());
    if (Objects.nonNull(expected.getProcessingStartedDate())) {
      context.assertEquals(expected.getProcessingStartedDate().getTime(), 
        actual.getProcessingStartedDate().getTime());
    }
  }

  public void assertNoopQueryResults(TestContext context, SnapshotCollection actual) {
    List<Snapshot> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  public void assertArbitruaryQueryResults(TestContext context, SnapshotCollection actual) {
    List<Snapshot> expected = getMockEntities().stream()
      .filter(entity -> entity.getStatus().equals(getArbitruaryQuery().getStatus()))
      .collect(Collectors.toList());
    Collections.sort(expected, (s1, s2) -> s1.getStatus().compareTo(s2.getStatus()));
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  public void assertArbitruarySortedQueryResults(TestContext context, SnapshotCollection actual) {
    List<Snapshot> expected = getMockEntities().stream()
      .filter(entity -> entity.getStatus().equals(getArbitruaryQuery().getStatus()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedSnapshot -> context.assertTrue(actual.getSnapshots().stream()
      .anyMatch(actualSnapshot -> actualSnapshot.getJobExecutionId().equals(expectedSnapshot.getJobExecutionId()))));
  }

  public String getCompleteWhereClause() {
    return "WHERE id = '6681ef31-03fe-4abc-9596-23de06d575c5'" +
      " AND status = 'PROCESSING_IN_PROGRESS'";
  }

  public static LBSnapshotMocks mock() {
    return new LBSnapshotMocks();
  }

}