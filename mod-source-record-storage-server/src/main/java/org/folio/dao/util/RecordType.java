package org.folio.dao.util;

public enum RecordType {

  MARC("marc_records");

  private String tableName;

  RecordType(String tableName){
    this.tableName = tableName;
  }
  public String getTableName() {
    return tableName;
  }

}
