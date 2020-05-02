package org.folio.dao.impl;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.folio.dao.ErrorRecordDao;
import org.folio.dao.filter.ErrorRecordFilter;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ErrorRecordCollection;
import org.junit.runner.RunWith;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class ErrorRecordDaoTest extends AbstractRecordDaoTest<ErrorRecord, ErrorRecordCollection, ErrorRecordFilter, ErrorRecordDao> {

  @Override
  public void createDao(TestContext context) {
    dao = new ErrorRecordDaoImpl(postgresClientFactory);
  }

  @Override
  public ErrorRecordFilter getNoopFilter() {
    return new ErrorRecordFilter();
  }

  @Override
  public ErrorRecordFilter getArbitruaryFilter() {
    ErrorRecordFilter snapshotFilter = new ErrorRecordFilter();
    snapshotFilter.setDescription("Oops... something happened");
    return snapshotFilter;
  }

  @Override
  public ErrorRecord getMockBean() {
    return MockErrorRecordFactory.getMockErrorRecord(mockRecord);
  }

  @Override
  public ErrorRecord getInvalidMockBean() {
    return new ErrorRecord()
      .withId(mockRecord.getId());
  }

  @Override
  public ErrorRecord getUpdatedMockBean() {
    return getMockBean()
      .withDescription("Something went really wrong");
  }

  @Override
  public ErrorRecord[] getMockBeans() {
    return MockErrorRecordFactory.getMockErrorRecords(mockRecords);
  }

  @Override
  public void compareBeans(TestContext context, ErrorRecord expected, ErrorRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getDescription(), actual.getDescription());
    context.assertEquals(new JsonObject((String) expected.getContent()), new JsonObject((String) actual.getContent()));
  }

  @Override
  public void assertNoopFilterResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = Arrays.asList(getMockBeans());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, ErrorRecordCollection actual) {
    List<ErrorRecord> expected = Arrays.asList(getMockBeans()).stream()
      .filter(bean -> bean.getDescription().equals(getArbitruaryFilter().getDescription()))
      .collect(Collectors.toList());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedErrorRecord -> context.assertTrue(actual.getErrorRecords().stream()
      .anyMatch(actualErrorRecord -> actualErrorRecord.getId().equals(expectedErrorRecord.getId()))));
  }

}