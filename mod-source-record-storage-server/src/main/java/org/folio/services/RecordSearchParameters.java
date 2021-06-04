package org.folio.services;

import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
import org.folio.rest.jaxrs.model.Record;

/**
 * Parameters needed for extended search
 *
 * @see MarcRecordSearchRequest
 */
public class RecordSearchParameters {
  private String leaderSearchExpression;
  private String fieldsSearchExpression;
  private Record.RecordType recordType;
  private boolean deleted;
  private boolean suppressedFromDiscovery;
  private Integer limit;
  private Integer offset;

  public static RecordSearchParameters from(MarcRecordSearchRequest request) {
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

  public String getLeaderSearchExpression() {
    return leaderSearchExpression;
  }

  public void setLeaderSearchExpression(String leaderSearchExpression) {
    this.leaderSearchExpression = leaderSearchExpression;
  }

  public String getFieldsSearchExpression() {
    return fieldsSearchExpression;
  }

  public void setFieldsSearchExpression(String fieldsSearchExpression) {
    this.fieldsSearchExpression = fieldsSearchExpression;
  }

  public Record.RecordType getRecordType() {
    return recordType;
  }

  public void setRecordType(Record.RecordType recordType) {
    this.recordType = recordType;
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  public boolean isSuppressedFromDiscovery() {
    return suppressedFromDiscovery;
  }

  public void setSuppressedFromDiscovery(boolean suppressedFromDiscovery) {
    this.suppressedFromDiscovery = suppressedFromDiscovery;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public Integer getOffset() {
    return offset;
  }

  public void setOffset(Integer offset) {
    this.offset = offset;
  }
}
