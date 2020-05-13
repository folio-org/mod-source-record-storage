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

  @Override
  public String getId(ParsedRecord parsedRecord) {
    return parsedRecord.getId();
  }

  @Override
  public ParsedRecordQuery getNoopQuery() {
    return new ParsedRecordQuery();
  }

  @Override
  public ParsedRecordQuery getArbitruaryQuery() {
    ParsedRecordQuery snapshotQuery = new ParsedRecordQuery();
    // NOTE: no reasonable field to filter on
    return snapshotQuery;
  }

  @Override
  public ParsedRecordQuery getArbitruarySortedQuery() {
    ParsedRecordQuery snapshotQuery = new ParsedRecordQuery();
    snapshotQuery.orderBy("id", Direction.DESC);
    return snapshotQuery;
  }

  @Override
  public ParsedRecord getMockEntity() {
    return TestMocks.getParsedRecord(0);
  }

  @Override
  public ParsedRecord getInvalidMockEntity() {
    return new ParsedRecord()
      .withId(TestMocks.getRecord(0).getId());
  }

  @Override
  public ParsedRecord getUpdatedMockEntity() {
    return new ParsedRecord()
      .withId(getMockEntity().getId())
      .withContent(getMockEntity().getContent())
      .withFormattedContent(getMockEntity().getFormattedContent());
  }

  @Override
  public List<ParsedRecord> getMockEntities() {
    return TestMocks.getParsedRecords();
  }

  @Override
  public ParsedRecordQuery getCompleteQuery() {
    ParsedRecordQuery query = new ParsedRecordQuery();
    BeanUtils.copyProperties(TestMocks.getParsedRecord("0f0fe962-d502-4a4f-9e74-7732bec94ee8").get(), query);
    return query;
  }

  @Override
  public String getCompleteWhereClause() {
    return "WHERE id = '0f0fe962-d502-4a4f-9e74-7732bec94ee8'";
  }

  @Override
  public ParsedRecord getExpectedEntity() {
    return getMockEntity();
  }

  @Override
  public ParsedRecord getExpectedUpdatedEntity() {
    return getUpdatedMockEntity();
  }

  @Override
  public List<ParsedRecord> getExpectedEntities() {
    return getMockEntities();
  }

  @Override
  public List<ParsedRecord> getExpectedEntitiesForArbitraryQuery() {
    return getExpectedEntities();
  }

  @Override
  public List<ParsedRecord> getExpectedEntitiesForArbitrarySortedQuery() {
    List<ParsedRecord> expected = getExpectedEntitiesForArbitraryQuery();
    Collections.sort(expected, (pr1, pr2) -> pr2.getId().compareTo(pr1.getId()));
    return expected;
  }

  @Override
  public ParsedRecordCollection getExpectedCollection() {
    List<ParsedRecord> expected = getExpectedEntities();
    return new ParsedRecordCollection()
      .withParsedRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public ParsedRecordCollection getExpectedCollectionForArbitraryQuery() {
    List<ParsedRecord> expected = getExpectedEntitiesForArbitraryQuery();
    return new ParsedRecordCollection()
      .withParsedRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public ParsedRecordCollection getExpectedCollectionForArbitrarySortedQuery() {
    List<ParsedRecord> expected = getExpectedEntitiesForArbitrarySortedQuery();
    return new ParsedRecordCollection()
      .withParsedRecords(expected)
      .withTotalRecords(expected.size());
  }

  @Override
  public void assertEmptyResult(TestContext context, int expectedTotal, ParsedRecordCollection actual) {
    context.assertEquals(new Integer(expectedTotal), actual.getTotalRecords());
    context.assertTrue(actual.getParsedRecords().isEmpty());
  }

  @Override
  public void compareCollections(TestContext context, ParsedRecordCollection expected, ParsedRecordCollection actual) {
    context.assertEquals(expected.getTotalRecords(), actual.getTotalRecords());
    compareEntities(context, expected.getParsedRecords(), actual.getParsedRecords());
  }

  @Override
  public void compareEntities(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(new JsonObject((String) expected.getContent()),
      new JsonObject((String) actual.getContent()));
    context.assertEquals(expected.getFormattedContent().trim(), actual.getFormattedContent().trim());
  }

  public static ParsedRecordMocks mock() {
    return new ParsedRecordMocks();
  }

}