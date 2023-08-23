package org.folio.dao.util;

public enum IdType {

  INSTANCE("instanceId"),
  HOLDINGS("holdingsId"),
  AUTHORITY("authorityId"),
  EXTERNAL("externalId"),
  // NOTE: not really external id but is default from dto
  RECORD("id");

  private final String idField;

  IdType(String idField) {
    this.idField = idField;
  }

  public String getIdField() {
    return idField;
  }

}
