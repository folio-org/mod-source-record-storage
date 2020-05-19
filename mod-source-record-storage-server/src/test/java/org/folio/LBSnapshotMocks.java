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

import io.vertx.ext.unit.TestContext;

public class LBSnapshotMocks implements EntityMocks<Snapshot, SnapshotCollection, SnapshotQuery> {

  private LBSnapshotMocks() { }

  @Override
  public String getId(Snapshot snapshot) {
    return snapshot.getJobExecutionId();
  }

  @Override
  public SnapshotQuery getNoopQuery() {
    return SnapshotQuery.query();
  }

  @Override
  public SnapshotQuery getArbitruaryQuery() {
    return SnapshotQuery.query().builder()
      .whereEqual("status", Snapshot.Status.NEW)
      .query();
  }

  @Override
  public SnapshotQuery getArbitruarySortedQuery() {
    return SnapshotQuery.query().builder()
      .whereEqual("status", Snapshot.Status.NEW)
      .orderBy("status")
      .query();
  }

  @Override
  public Snapshot getMockEntity() {
    return TestMocks.getSnapshot(0);
  }

  @Override
  public Snapshot getInvalidMockEntity() {
    return new Snapshot()
      .withJobExecutionId(getMockEntity().getJobExecutionId());
  }

  @Override
  public Snapshot getUpdatedMockEntity() {
    return new Snapshot()
      .withJobExecutionId(getMockEntity().getJobExecutionId())
      .withStatus(Snapshot.Status.PARSING_IN_PROGRESS)
      .withProcessingStartedDate(new Date(1589218979000l))
      .withMetadata(getMockEntity().getMetadata());
  }

  @Override
  public List<Snapshot> getMockEntities() {
    return TestMocks.getSnapshots();
  }

  @Override
  public Snapshot getExpectedEntity() {
    return getMockEntity();
  }

  @Override
  public Snapshot getExpectedUpdatedEntity() {
    return getUpdatedMockEntity();
  }

  @Override
  public List<Snapshot> getExpectedEntities() {
    return getMockEntities();
  }

  @Override
  public List<Snapshot> getExpectedEntitiesForArbitraryQuery() {
    return getExpectedEntities().stream()
      .filter(entity -> entity.getStatus().equals(Snapshot.Status.NEW))
      .collect(Collectors.toList());
  }

  @Override
  public List<Snapshot> getExpectedEntitiesForArbitrarySortedQuery() {
    List<Snapshot> expected = getExpectedEntitiesForArbitraryQuery();
    Collections.sort(expected, (s1, s2) -> s1.getStatus().compareTo(s2.getStatus()));
    return expected;
  }

  @Override
  public SnapshotCollection getExpectedCollection() {
    List<Snapshot> expected = getExpectedEntities();
    return new SnapshotCollection()
      .withSnapshots(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public SnapshotCollection getExpectedCollectionForArbitraryQuery() {
    List<Snapshot> expected = getExpectedEntitiesForArbitraryQuery();
    return new SnapshotCollection()
      .withSnapshots(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public SnapshotCollection getExpectedCollectionForArbitrarySortedQuery() {
    List<Snapshot> expected = getExpectedEntitiesForArbitrarySortedQuery();
    return new SnapshotCollection()
      .withSnapshots(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public void assertEmptyResult(TestContext context, int expectedTotal, SnapshotCollection actual) {
    context.assertEquals(new Integer(expectedTotal), actual.getTotalRecords());
    context.assertTrue(actual.getSnapshots().isEmpty());
  }

  @Override
  public void compareCollections(TestContext context, SnapshotCollection expected, SnapshotCollection actual) {
    context.assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    compareEntities(context, expected.getSnapshots(), actual.getSnapshots(), false);
  }

  @Override
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
    if (Objects.nonNull(expected.getMetadata())) {
      context.assertEquals(expected.getMetadata().getCreatedByUserId(), actual.getMetadata().getCreatedByUserId());
      // context.assertNotNull(actual.getMetadata().getCreatedDate());
      context.assertEquals(expected.getMetadata().getUpdatedByUserId(), actual.getMetadata().getUpdatedByUserId());
      // context.assertNotNull(actual.getMetadata().getUpdatedDate());
    }
  }

  public static LBSnapshotMocks mock() {
    return new LBSnapshotMocks();
  }

}