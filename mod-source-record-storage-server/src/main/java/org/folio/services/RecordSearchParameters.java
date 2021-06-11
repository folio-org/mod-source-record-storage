package org.folio.services;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
import org.folio.rest.jaxrs.model.Record;

/**
 * Parameters needed for extended search
 *
 * @see MarcRecordSearchRequest
 */
@Getter(AccessLevel.PUBLIC)
@Setter(AccessLevel.PRIVATE)
public class RecordSearchParameters {
  private String leaderSearchExpression;
  private String fieldsSearchExpression;
  private Record.RecordType recordType;
  private boolean deleted;
  private boolean suppressedFromDiscovery;
  private Integer limit;
  private Integer offset;

  public static RecordSearchParameters from(MarcRecordSearchRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("Failed to create RecordSearchParameters: the incoming MarcRecordSearchRequest is null");
    }
    RecordSearchParameters params = new RecordSearchParameters();
    params.setLeaderSearchExpression(request.getLeaderSearchExpression());
    params.setFieldsSearchExpression(request.getFieldsSearchExpression());
    /* Temporary set record type to MARC_BIB. This parameter will come from Http request later. See MODSOURCE-281 */
    params.setRecordType(Record.RecordType.MARC_BIB);
    params.setDeleted(request.getDeleted());
    params.setSuppressedFromDiscovery(request.getSuppressFromDiscovery());
    params.setLimit(request.getLimit());
    params.setOffset(request.getOffset());
    return params;
  }
}
