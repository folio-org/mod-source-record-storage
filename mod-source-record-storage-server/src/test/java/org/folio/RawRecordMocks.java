package org.folio;

import java.util.Collections;
import java.util.List;

import org.folio.dao.query.RawRecordQuery;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.springframework.beans.BeanUtils;

import io.vertx.ext.unit.TestContext;

public class RawRecordMocks implements EntityMocks<RawRecord, RawRecordCollection, RawRecordQuery> {

  private RawRecordMocks() { }

  public String getId(RawRecord rawRecord) {
    return rawRecord.getId();
  }

  public RawRecordQuery getNoopQuery() {
    return new RawRecordQuery();
  }

  public RawRecordQuery getArbitruaryQuery() {
    RawRecordQuery snapshotQuery = new RawRecordQuery();
    // NOTE: no reasonable field to filter on
    return snapshotQuery;
  }

  public RawRecordQuery getArbitruarySortedQuery() {
    return (RawRecordQuery) getArbitruaryQuery()
      .orderBy("id");
  }

  public RawRecord getMockEntity() {
    return TestMocks.getRawRecord(0);
  }

  public RawRecord getInvalidMockEntity() {
    return new RawRecord()
      .withId(TestMocks.getRecord(0).getId());
  }

  public RawRecord getUpdatedMockEntity() {
    return new RawRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent());
  }

  public List<RawRecord> getMockEntities() {
    return TestMocks.getRawRecords();
  }

  public void compareEntities(TestContext context, RawRecord expected, RawRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
  }

  public void assertNoopQueryResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  public void assertArbitruaryQueryResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  public void assertArbitruarySortedQueryResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    Collections.sort(expected, (rr1, rr2) -> rr1.getId().compareTo(rr2.getId()));
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  public RawRecordQuery getCompleteQuery() {
    RawRecordQuery query = new RawRecordQuery();
    BeanUtils.copyProperties(TestMocks.getRawRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    return query;
  }

  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'";
  }

  public static RawRecordMocks mock() {
    return new RawRecordMocks();
  }

}