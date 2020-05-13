package org.folio;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.folio.dao.query.RecordQuery;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.springframework.beans.BeanUtils;

import io.vertx.ext.unit.TestContext;

public class LBRecordMocks implements EntityMocks<Record, RecordCollection, RecordQuery> {

  private LBRecordMocks() { }

  @Override
  public String getId(Record record) {
    return record.getId();
  }

  @Override
  public RecordQuery getNoopQuery() {
    return new RecordQuery();
  }

  @Override
  public RecordQuery getArbitruaryQuery() {
    RecordQuery snapshotQuery = new RecordQuery();
    snapshotQuery.setMatchedProfileId(getMockEntity().getMatchedProfileId());
    snapshotQuery.setState(Record.State.ACTUAL);
    return snapshotQuery;
  }

  @Override
  public RecordQuery getArbitruarySortedQuery() {
    RecordQuery snapshotQuery = new RecordQuery();
    snapshotQuery.setMatchedProfileId(getMockEntity().getMatchedProfileId());
    // snapshotQuery.setState(Record.State.ACTUAL);
    snapshotQuery.orderBy("matchedProfileId");
    return snapshotQuery;
  }

  @Override
  public RecordQuery getCompleteQuery() {
    RecordQuery query = new RecordQuery();
    BeanUtils.copyProperties(TestMocks.getRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    query.withMetadata(query.getMetadata().withCreatedDate(null).withUpdatedDate(null));
    return query;
  }

  @Override
  public Record getMockEntity() {
    return TestMocks.getRecord(0);
  }

  @Override
  public Record getInvalidMockEntity() {
    return new Record()
      .withId("be86e5ad-d0ac-4209-af40-875ffd9efa53")
      .withRecordType(Record.RecordType.MARC)
      .withOrder(0)
      .withGeneration(1)
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
  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'" +
      " AND matchedid = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'" +
      " AND snapshotid = '7f939c0b-618c-4eab-8276-a14e0bfe5728'" +
      " AND matchedprofileid = '0731b68a-147e-4ad8-9de2-7eef7c1a5a99'" +
      " AND generation = 0" +
      " AND orderinfile = 1" +
      " AND recordtype = 'MARC'" +
      " AND state = 'ACTUAL'" +
      " AND instanceid = '6b4ae089-e1ee-431f-af83-e1133f8e3da0'" +
      " AND suppressdiscovery = false" +
      " AND createdbyuserid = '4547e8af-638a-4595-8af8-4d396d6a9f7a'" +
      " AND updatedbyuserid = '4547e8af-638a-4595-8af8-4d396d6a9f7a'";
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
      .filter(entity -> entity.getState().equals(getArbitruaryQuery().getState()) &&
        entity.getMatchedProfileId().equals(getArbitruaryQuery().getMatchedProfileId()))
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
    compareEntities(context, expected.getRecords(), actual.getRecords());
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