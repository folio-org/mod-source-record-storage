package org.folio.dao.impl;

import java.util.Arrays;
import java.util.List;

import org.folio.dao.RawRecordDao;
import org.folio.dao.filter.RawRecordFilter;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordCollection;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class RawRecordDaoTest extends AbstractRecordDaoTest<RawRecord, RawRecordCollection, RawRecordFilter, RawRecordDao> {

  @Override
  public void createDao(TestContext context) {
    dao = new RawRecordDaoImpl(postgresClientFactory);
  }

  @Override
  public RawRecordFilter getNoopFilter() {
    return new RawRecordFilter();
  }

  @Override
  public RawRecordFilter getArbitruaryFilter() {
    RawRecordFilter snapshotFilter = new RawRecordFilter();
    // NOTE: no reasonable field to filter on
    return snapshotFilter;
  }

  @Override
  public RawRecord getMockBean() {
    return MockRawRecordFactory.getMockRawRecord(mockRecord);
  }

  @Override
  public RawRecord getInvalidMockBean() {
    return new RawRecord()
      .withId(mockRecord.getId());
  }

  @Override
  public RawRecord getUpdatedMockBean() {
    return getMockBean();
  }

  @Override
  public RawRecord[] getMockBeans() {
    return MockRawRecordFactory.getMockRawRecords(mockRecords);
  }

  @Override
  public void compareBeans(TestContext context, RawRecord expected, RawRecord actual) {
    context.assertEquals(expected.getId(), actual.getId());
    context.assertEquals(expected.getContent(), actual.getContent());
  }

  @Override
  public void assertNoopFilterResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = Arrays.asList(getMockBeans());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

  @Override
  public void assertArbitruaryFilterResults(TestContext context, RawRecordCollection actual) {
    List<RawRecord> expected = Arrays.asList(getMockBeans());
    context.assertEquals(new Integer(expected.size()), actual.getTotalRecords());
    expected.forEach(expectedRawRecord -> context.assertTrue(actual.getRawRecords().stream()
      .anyMatch(actualRawRecord -> actualRawRecord.getId().equals(expectedRawRecord.getId()))));
  }

}