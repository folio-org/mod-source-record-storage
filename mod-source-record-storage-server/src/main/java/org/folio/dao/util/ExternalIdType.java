package org.folio.dao.util;

public enum  ExternalIdType {
  INSTANCE("INSTANCE", "instanceId");

  private String idType;
  private String externalIdField;

  ExternalIdType(String idType, String externalIdField) {
    this.idType = idType;
    this.externalIdField = externalIdField;
  }

  public String getExternalIdField() {
    return externalIdField;
  }
}

