package org.folio;

import java.util.Collections;
import java.util.List;

import org.folio.dao.query.OrderBy.Direction;
import org.folio.dao.query.ParsedRecordQuery;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.springframework.beans.BeanUtils;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

public class ParsedRecordMocks implements EntityMocks<ParsedRecord, ParsedRecordCollection, ParsedRecordQuery> {

  private ParsedRecordMocks() { }

  public String getId(ParsedRecord parsedRecord) {
    return parsedRecord.getId();
  }

  public ParsedRecordQuery getNoopQuery() {
    return new ParsedRecordQuery();
  }

  public ParsedRecordQuery getArbitruaryQuery() {
    ParsedRecordQuery snapshotQuery = new ParsedRecordQuery();
    // NOTE: no reasonable field to filter on
    return snapshotQuery;
  }

  public ParsedRecordQuery getArbitruarySortedQuery() {
    return (ParsedRecordQuery) getArbitruaryQuery()
      .orderBy("id", Direction.DESC);
  }

  public ParsedRecord getMockEntity() {
    return TestMocks.getParsedRecord(0);
  }

  public ParsedRecord getInvalidMockEntity() {
    return new ParsedRecord()
      .withId(TestMocks.getRecord(0).getId());
  }

  public ParsedRecord getUpdatedMockEntity() {
    return new ParsedRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent());
  }

  public List<ParsedRecord> getMockEntities() {
    return TestMocks.getParsedRecords();
  }

  public void compareEntities(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(new JsonObject((String) expected.getContent()),
      new JsonObject((String) actual.getContent()));
    context.assertEquals(expected.getFormattedContent().trim(), actual.getFormattedContent().trim());
  }

  public void assertNoopQueryResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  public void assertArbitruaryQueryResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockEntities();
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  public void assertArbitruarySortedQueryResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = getMockEntities();
    Collections.sort(expected, (pr1, pr2) -> pr2.getId().compareTo(pr1.getId()));
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  public ParsedRecordQuery getCompleteQuery() {
    ParsedRecordQuery query = new ParsedRecordQuery();
    BeanUtils.copyProperties(TestMocks.getParsedRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    return query;
  }

  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'";
  }

  public static ParsedRecordMocks mock() {
    return new ParsedRecordMocks();
  }

}