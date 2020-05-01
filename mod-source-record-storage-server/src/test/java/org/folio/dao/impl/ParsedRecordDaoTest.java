package org.folio.dao.impl;

import java.util.Arrays;
import java.util.List;

import org.folio.dao.ParsedRecordDao;
import org.folio.dao.filter.ParsedRecordFilter;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordCollection;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ParsedRecordDaoTest extends AbstractRecordDaoTest<ParsedRecord, ParsedRecordCollection, ParsedRecordFilter, ParsedRecordDao> {

  @Override
  public void createDao(TestContext context) {
    dao = new ParsedRecordDaoImpl(postgresClientFactory);
  }

  @Override
  public ParsedRecordFilter getNoopFilter() {
    return new ParsedRecordFilter();
  }

  @Override
  public ParsedRecordFilter getArbitruaryFilter() {
    ParsedRecordFilter snapshotFilter = new ParsedRecordFilter();
    // NOTE: no reasonable field to filter on
    return snapshotFilter;
  }

  @Override
  public ParsedRecord getMockBean() {
    return new ParsedRecord()
      .withId(mockRecord.getId())
      .withContent(parsedMarc);
  }

  @Override
  public ParsedRecord getMockBeanWithoutId() {
    return new ParsedRecord()
      .withContent(parsedMarc);
  }

  @Override
  public ParsedRecord getInvalidMockBean() {
    return new ParsedRecord()
      .withId(mockRecord.getId());
  }

  @Override
  public ParsedRecord getUpdatedMockBean() {
    return getMockBean();
  }

  @Override
  public ParsedRecord[] getMockBeans() {
    return new ParsedRecord[] {
      new ParsedRecord()
        .withId(mockRecords[0].getId())
        .withContent(parsedMarc),
      new ParsedRecord()
        .withId(mockRecords[1].getId())
        .withContent(parsedMarc),
      new ParsedRecord()
        .withId(mockRecords[2].getId())
        .withContent(parsedMarc),
      new ParsedRecord()
        .withId(mockRecords[3].getId())
        .withContent(parsedMarc),
      new ParsedRecord()
        .withId(mockRecords[4].getId())
        .withContent(parsedMarc)
    };
  }

  @Override
  public void compareBeans(TestContext context, ParsedRecord expected, ParsedRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(new JsonObject((String) expected.getContent()), new JsonObject((String) actual.getContent()));
  }

  @Override
  public void assertTotal(TestContext context, Integer expected, ParsedRecordCollection actual) {
    context.assertEquals(expected, actual.getTotalRecords());
  }

  @Override
  public void assertNoopFilterResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = Arrays.asList(getMockBeans());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, ParsedRecordCollection actual) {
    List<ParsedRecord> expected = Arrays.asList(getMockBeans());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedParsedRecord -> context.assertTrue(actual.getParsedRecords().stream()
      .anyMatch(actualParsedRecord -> actualParsedRecord.getId().equals(expectedParsedRecord.getId()))));
  }

}