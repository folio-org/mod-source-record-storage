package org.folio.dao.util;

public enum  ExternalIdType {
  INSTANCE("instanceId");

  private String externalIdField;

  ExternalIdType(String externalIdField) {
    this.externalIdField = externalIdField;
  }

  public String getExternalIdField() {
    return externalIdField;
  }
}

