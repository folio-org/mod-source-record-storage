package org.folio.dao.util;

public enum  ExternalIdType {

  INSTANCE("instanceId"),
  // NOTE: not really external id but is default from dto
  RECORD("id");

  private String externalIdField;

  ExternalIdType(String externalIdField) {
    this.externalIdField = externalIdField;
  }

  public String getExternalIdField() {
    return externalIdField;
  }
}
