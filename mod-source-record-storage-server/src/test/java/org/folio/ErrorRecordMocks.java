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

  public String getId(ErrorRecord errorRecord) {
    return errorRecord.getId();
  }

  public ErrorRecordQuery getNoopQuery() {
    return new ErrorRecordQuery();
  }

  public ErrorRecordQuery getArbitruaryQuery() {
    ErrorRecordQuery snapshotQuery = new ErrorRecordQuery();
    snapshotQuery.setDescription(getMockEntity().getDescription());
    return snapshotQuery;
  }

  public ErrorRecordQuery getArbitruarySortedQuery() {
    return (ErrorRecordQuery) getArbitruaryQuery()
      .orderBy("description");
  }

  public ErrorRecordQuery getCompleteQuery() {
    ErrorRecordQuery query = new ErrorRecordQuery();
    BeanUtils.copyProperties(TestMocks.getErrorRecord("d3cd3e1e-a18c-4f7c-b053-9aa50343394e").get(), query);
    return query;
  }

  public ErrorRecord getMockEntity() {
    return TestMocks.getErrorRecord(0);
  }

  public ErrorRecord getInvalidMockEntity() {
    return new ErrorRecord().withId(TestMocks.getRecord(0).getId());
  }

  public ErrorRecord getUpdatedMockEntity() {
    return new ErrorRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent())
      .withDescription("Something went really wrong");
  }

  public List<ErrorRecord> getMockEntities() {
    return TestMocks.getErrorRecords();
  }

  public void compareEntities(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getDescription(), actual.getDescription());
    context.assertEquals(new JsonObject((String) expected.getContent()), new JsonObject((String) actual.getContent()));
  }

  public void assertNoopQueryResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  public void assertArbitruaryQueryResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockEntities().stream()
      .filter(entity -> entity.getDescription().equals(getArbitruaryQuery().getDescription()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  public void assertArbitruarySortedQueryResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = getMockEntities().stream()
      .filter(entity -> entity.getDescription().equals(getArbitruarySortedQuery().getDescription()))
      .collect(Collectors.toList());
    Collections.sort(expected, (er1, er2) -> er1.getDescription().compareTo(er2.getDescription()));
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  public String getCompleteWhereClause() {
    return "WHERE id = 'd3cd3e1e-a18c-4f7c-b053-9aa50343394e'" + " AND description = 'Opps... something went wrong'";
  }

  public static ErrorRecordMocks mock() {
    return new ErrorRecordMocks();
  }

}