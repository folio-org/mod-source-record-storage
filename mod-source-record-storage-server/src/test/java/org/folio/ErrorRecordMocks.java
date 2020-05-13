package org.folio;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.folio.dao.query.ErrorRecordQuery;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.springframework.beans.BeanUtils;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

public class ErrorRecordMocks implements EntityMocks<ErrorRecord, ErrorRecordCollection, ErrorRecordQuery> {

  private ErrorRecordMocks() { }

  @Override
  public String getId(ErrorRecord errorRecord) {
    return errorRecord.getId();
  }

  @Override
  public ErrorRecordQuery getNoopQuery() {
    return new ErrorRecordQuery();
  }

  @Override
  public ErrorRecordQuery getArbitruaryQuery() {
    ErrorRecordQuery snapshotQuery = new ErrorRecordQuery();
    snapshotQuery.setDescription(getMockEntity().getDescription());
    return snapshotQuery;
  }

  @Override
  public ErrorRecordQuery getArbitruarySortedQuery() {
    ErrorRecordQuery snapshotQuery = new ErrorRecordQuery();
    snapshotQuery.setDescription(getMockEntity().getDescription());
    snapshotQuery.orderBy("description");
    return snapshotQuery;
  }

  @Override
  public ErrorRecordQuery getCompleteQuery() {
    ErrorRecordQuery query = new ErrorRecordQuery();
    BeanUtils.copyProperties(TestMocks.getErrorRecord("d3cd3e1e-a18c-4f7c-b053-9aa50343394e").get(), query);
    return query;
  }

  @Override
  public ErrorRecord getMockEntity() {
    return TestMocks.getErrorRecord(0);
  }

  @Override
  public ErrorRecord getInvalidMockEntity() {
    return new ErrorRecord()
      .withId(TestMocks.getRecord(0).getId());
  }

  @Override
  public ErrorRecord getUpdatedMockEntity() {
    return new ErrorRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent())
      .withDescription("Something went really wrong");
  }

  @Override
  public List<ErrorRecord> getMockEntities() {
    return TestMocks.getErrorRecords();
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = 'd3cd3e1e-a18c-4f7c-b053-9aa50343394e'"
      + " AND description = 'Opps... something went wrong'";
  }

  @Override
  public ErrorRecord getExpectedEntity() {
    return getMockEntity();
  }

  @Override
  public ErrorRecord getExpectedUpdatedEntity() {
    return getUpdatedMockEntity();
  }

  @Override
  public List<ErrorRecord> getExpectedEntities() {
    return getMockEntities();
  }

  @Override
  public List<ErrorRecord> getExpectedEntitiesForArbitraryQuery() {
    return getExpectedEntities().stream()
      .filter(entity -> entity.getDescription().equals(getArbitruaryQuery().getDescription()))
      .collect(Collectors.toList());
  }

  @Override
  public List<ErrorRecord> getExpectedEntitiesForArbitrarySortedQuery() {
    List<ErrorRecord> expected = getExpectedEntitiesForArbitraryQuery();
    Collections.sort(expected, (er1, er2) -> er1.getDescription().compareTo(er2.getDescription()));
    return expected;
  }

  @Override
  public ErrorRecordCollection getExpectedCollection() {
    List<ErrorRecord> expected = getExpectedEntities();
    return new ErrorRecordCollection()
      .withErrorRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public ErrorRecordCollection getExpectedCollectionForArbitraryQuery() {
    List<ErrorRecord> expected = getExpectedEntitiesForArbitraryQuery();
    return new ErrorRecordCollection()
      .withErrorRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public ErrorRecordCollection getExpectedCollectionForArbitrarySortedQuery() {
    List<ErrorRecord> expected = getExpectedEntitiesForArbitrarySortedQuery();
    return new ErrorRecordCollection()
      .withErrorRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public void assertEmptyResult(TestContext context, int expectedTotal, ErrorRecordCollection actual) {
    context.assertEquals(new Integer(expectedTotal), actual.getTotalRecords());
    context.assertTrue(actual.getErrorRecords().isEmpty());
  }

  @Override
  public void compareCollections(TestContext context, ErrorRecordCollection expected, ErrorRecordCollection actual) {
    context.assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    compareEntities(context, expected.getErrorRecords(), actual.getErrorRecords());
  }

  @Override
  public void compareEntities(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getDescription(), actual.getDescription());
    context.assertEquals(new JsonObject((String) expected.getContent()), new JsonObject((String) actual.getContent()));
  }

  public static ErrorRecordMocks mock() {
    return new ErrorRecordMocks();
  }

}