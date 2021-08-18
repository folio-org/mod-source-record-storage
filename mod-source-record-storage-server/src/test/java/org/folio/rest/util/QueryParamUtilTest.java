package org.folio.rest.util;

import static org.junit.Assert.assertEquals;

import javax.ws.rs.BadRequestException;

import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

@RunWith(BlockJUnit4ClassRunner.class)
public class QueryParamUtilTest {

  @Test
  public void shouldReturnRecordExternalIdType() {
    assertEquals(IdType.RECORD, QueryParamUtil.toExternalIdType("RECORD"));
  }

  @Test
  public void shouldReturnExternalIdTypeOnInstance() {
    assertEquals(IdType.INSTANCE, QueryParamUtil.toExternalIdType("INSTANCE"));
  }

  @Test
  public void shouldReturnExternalIdTypeOnHoldings() {
    assertEquals(IdType.HOLDINGS, QueryParamUtil.toExternalIdType("HOLDINGS"));
  }

  @Test
  public void shouldReturnDefaultExternalIdType() {
    assertEquals(IdType.RECORD, QueryParamUtil.toExternalIdType(null));
    assertEquals(IdType.RECORD, QueryParamUtil.toExternalIdType(""));
  }

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionForUnknownExternalIdType() {
    QueryParamUtil.toExternalIdType("UNKNOWN");
  }

  @Test
  public void shouldReturnMarcBibRecordType() {
    assertEquals(RecordType.MARC_BIB, QueryParamUtil.toRecordType("MARC_BIB"));
  }

  @Test
  public void shouldReturnEdifactRecordType() {
    assertEquals(RecordType.EDIFACT, QueryParamUtil.toRecordType("EDIFACT"));
  }

  @Test
  public void shouldReturnMarcAuthorityRecordType() {
    assertEquals(RecordType.MARC_AUTHORITY, QueryParamUtil.toRecordType("MARC_AUTHORITY"));
  }

  @Test
  public void shouldReturnMarcHoldingsRecordType() {
    assertEquals(RecordType.MARC_HOLDING, QueryParamUtil.toRecordType("MARC_HOLDING"));
  }

  @Test
  public void shouldReturnDefaultRecordType() {
    assertEquals(RecordType.MARC_BIB, QueryParamUtil.toRecordType(null));
    assertEquals(RecordType.MARC_BIB, QueryParamUtil.toRecordType(""));
  }

  @Test(expected = BadRequestException.class)
  public void shouldThrowBadRequestExceptionForUnknownRecordType() {
    QueryParamUtil.toRecordType("UNKNOWN");
  }

}
