package org.folio.dao.util;

/**
 * Enum used to distingush table for parsed record.
 */
public enum RecordType {

  MARC("marc_records_lb");

  private String tableName;

  RecordType(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

}
