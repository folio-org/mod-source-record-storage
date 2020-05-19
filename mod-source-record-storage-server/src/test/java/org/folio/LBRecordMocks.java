package org.folio;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;

import io.vertx.ext.unit.TestContext;

public class LBRecordMocks implements EntityMocks<Record, RecordCollection, RecordQuery> {

  private LBRecordMocks() { }

  @Override
  public String getId(Record record) {
    return record.getId();
  }

  @Override
  public RecordQuery getNoopQuery() {
    return RecordQuery.query();
  }

  @Override
  public RecordQuery getArbitruaryQuery() {
    return RecordQuery.query().builder()
      .whereEqual("matchedProfileId", getMockEntity().getMatchedProfileId())
      .query();
  }

  @Override
  public RecordQuery getArbitruarySortedQuery() {
    return RecordQuery.query().builder()
      .whereEqual("matchedProfileId", getMockEntity().getMatchedProfileId())
      .orderBy("matchedProfileId")
      .query();
  }

  @Override
  public Record getMockEntity() {
    return TestMocks.getRecord(0);
  }

  @Override
  public Record getInvalidMockEntity() {
    return new Record()
      .withId(getMockEntity().getId())
      .withSnapshotId(getMockEntity().getSnapshotId())
      .withRecordType(Record.RecordType.MARC)
      .withOrder(0)
      .withGeneration(0)
      .withState(Record.State.ACTUAL);
  }

  @Override
  public Record getUpdatedMockEntity() {
    return new Record()
      .withId(getMockEntity().getId())
      .withMatchedId(getMockEntity().getMatchedId())
      .withMatchedProfileId(getMockEntity().getMatchedProfileId())
      .withSnapshotId(getMockEntity().getSnapshotId())
      .withGeneration(getMockEntity().getGeneration())
      .withRecordType(getMockEntity().getRecordType())
      .withAdditionalInfo(getMockEntity().getAdditionalInfo())
      .withExternalIdsHolder(getMockEntity().getExternalIdsHolder())
      .withMetadata(getMockEntity().getMetadata())
      .withState(Record.State.DRAFT)
      .withOrder(2);
  }

  @Override
  public List<Record> getMockEntities() {
    return TestMocks.getRecords();
  }

  @Override
  public Record getExpectedEntity() {
    return getMockEntity();
  }

  @Override
  public Record getExpectedUpdatedEntity() {
    return getUpdatedMockEntity();
  }

  @Override
  public List<Record> getExpectedEntities() {
    return getMockEntities();
  }

  @Override
  public List<Record> getExpectedEntitiesForArbitraryQuery() {
    return getExpectedEntities().stream()
      .filter(entity -> entity.getMatchedProfileId().equals(getMockEntity().getMatchedProfileId()))
      .collect(Collectors.toList());
  }

  @Override
  public List<Record> getExpectedEntitiesForArbitrarySortedQuery() {
    List<Record> expected = getExpectedEntitiesForArbitraryQuery();
    Collections.sort(expected, (r1, r2) -> r1.getMatchedProfileId().compareTo(r2.getMatchedProfileId()));
    return expected;
  }

  @Override
  public RecordCollection getExpectedCollection() {
    List<Record> expected = getExpectedEntities();
    return new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public RecordCollection getExpectedCollectionForArbitraryQuery() {
    List<Record> expected = getExpectedEntitiesForArbitraryQuery();
    return new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public RecordCollection getExpectedCollectionForArbitrarySortedQuery() {
    List<Record> expected = getExpectedEntitiesForArbitrarySortedQuery();
    return new RecordCollection()
      .withRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public void assertEmptyResult(TestContext context, int expectedTotal, RecordCollection actual) {
    context.assertEquals(new Integer(expectedTotal), actual.getTotalRecords());
    context.assertTrue(actual.getRecords().isEmpty());
  }

  @Override
  public void compareCollections(TestContext context, RecordCollection expected, RecordCollection actual) {
    context.assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    compareEntities(context, expected.getRecords(), actual.getRecords(), false);
  }

  @Override
  public void compareEntities(TestContext context, Record expected, Record actual) {
    // NOTE: mock DAOs do not generate ids or dates
    if (StringUtils.isEmpty(expected.getId())) {
      // context.assertNotNull(actual.getId());
    } else {
      context.assertEquals(expected.getId(), actual.getId());
    }
    if (StringUtils.isEmpty(expected.getMatchedId())) {
      // context.assertNotNull(actual.getMatchedId());
    } else {
      context.assertEquals(expected.getMatchedId(), actual.getMatchedId());
    }
    context.assertEquals(expected.getSnapshotId(), actual.getSnapshotId());
    context.assertEquals(expected.getMatchedProfileId(), actual.getMatchedProfileId());
    context.assertEquals(expected.getGeneration(), actual.getGeneration());
    context.assertEquals(expected.getOrder(), actual.getOrder());
    context.assertEquals(expected.getState(), actual.getState());
    context.assertEquals(expected.getRecordType(), actual.getRecordType());
    if (Objects.nonNull(expected.getAdditionalInfo())) {
      context.assertEquals(expected.getAdditionalInfo().getSuppressDiscovery(), actual.getAdditionalInfo().getSuppressDiscovery());
    }
    if (Objects.nonNull(expected.getExternalIdsHolder())) {
      context.assertEquals(expected.getExternalIdsHolder().getInstanceId(), actual.getExternalIdsHolder().getInstanceId());
    }
    if (Objects.nonNull(expected.getMetadata())) {
      context.assertEquals(expected.getMetadata().getCreatedByUserId(), actual.getMetadata().getCreatedByUserId());
      // context.assertNotNull(actual.getMetadata().getCreatedDate());
      context.assertEquals(expected.getMetadata().getUpdatedByUserId(), actual.getMetadata().getUpdatedByUserId());
      // context.assertNotNull(actual.getMetadata().getUpdatedDate());
    }
  }

  public static LBRecordMocks mock() {
    return new LBRecordMocks();
  }

}