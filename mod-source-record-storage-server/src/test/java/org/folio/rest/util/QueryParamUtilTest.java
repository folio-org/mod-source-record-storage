package org.folio.rest.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.ws.rs.BadRequestException;

import org.folio.dao.util.IdType;
import org.folio.dao.util.RecordType;
import org.folio.rest.jooq.enums.RecordState;
import org.junit.jupiter.api.Test;

public class QueryParamUtilTest {

  @Test
  void shouldReturnRecordExternalIdType() {
    assertEquals(IdType.RECORD, QueryParamUtil.toExternalIdType("RECORD"));
  }

  @Test
  void shouldReturnExternalIdTypeOnInstance() {
    assertEquals(IdType.INSTANCE, QueryParamUtil.toExternalIdType("INSTANCE"));
  }

  @Test
  void shouldReturnExternalIdTypeOnHoldings() {
    assertEquals(IdType.HOLDINGS, QueryParamUtil.toExternalIdType("HOLDINGS"));
  }

  @Test
  void shouldReturnExternalIdTypeOnAuthority() {
    assertEquals(IdType.AUTHORITY, QueryParamUtil.toExternalIdType("AUTHORITY"));
  }

  @Test
  void shouldReturnDefaultExternalIdType() {
    assertEquals(IdType.RECORD, QueryParamUtil.toExternalIdType(null));
    assertEquals(IdType.RECORD, QueryParamUtil.toExternalIdType(""));
  }

  @Test
  void shouldThrowBadRequestExceptionForUnknownExternalIdType() {
    assertThrows(BadRequestException.class, () -> QueryParamUtil.toExternalIdType("UNKNOWN"));
  }

  @Test
  void shouldReturnMarcBibRecordType() {
    assertEquals(RecordType.MARC_BIB, QueryParamUtil.toRecordType("MARC_BIB"));
  }

  @Test
  void shouldReturnEdifactRecordType() {
    assertEquals(RecordType.EDIFACT, QueryParamUtil.toRecordType("EDIFACT"));
  }

  @Test
  void shouldReturnMarcAuthorityRecordType() {
    assertEquals(RecordType.MARC_AUTHORITY, QueryParamUtil.toRecordType("MARC_AUTHORITY"));
  }

  @Test
  void shouldReturnMarcHoldingsRecordType() {
    assertEquals(RecordType.MARC_HOLDING, QueryParamUtil.toRecordType("MARC_HOLDING"));
  }

  @Test
  void shouldReturnDefaultRecordType() {
    assertEquals(RecordType.MARC_BIB, QueryParamUtil.toRecordType(null));
    assertEquals(RecordType.MARC_BIB, QueryParamUtil.toRecordType(""));
  }

  @Test
  void shouldThrowBadRequestExceptionForUnknownRecordType() {
    assertThrows(BadRequestException.class, () -> QueryParamUtil.toRecordType("UNKNOWN"));
  }

  @Test
  void shouldReturnActualRecordState() {
    assertEquals(RecordState.ACTUAL, QueryParamUtil.toRecordState("ACTUAL"));
  }

  @Test
  void shouldReturnOldRecordState() {
    assertEquals(RecordState.OLD, QueryParamUtil.toRecordState("OLD"));
  }

  @Test
  void shouldReturnDeletedRecordState() {
    assertEquals(RecordState.DELETED, QueryParamUtil.toRecordState("DELETED"));
  }

  @Test
  void shouldReturnDraftRecordState() {
    assertEquals(RecordState.DRAFT, QueryParamUtil.toRecordState("DRAFT"));
  }

  @Test
  void shouldReturnDefaultRecordState() {
    assertEquals(RecordState.ACTUAL, QueryParamUtil.toRecordState(null));
    assertEquals(RecordState.ACTUAL, QueryParamUtil.toRecordState(""));
  }

  @Test
  void shouldThrowBadRequestExceptionForUnknownRecordState() {
    assertThrows(BadRequestException.class, () -> QueryParamUtil.toRecordState("UNKNOWN"));
  }
}
