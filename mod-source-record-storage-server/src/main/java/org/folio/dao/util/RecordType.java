package org.folio.dao.util;

/**
 * Enum used to distingush table for parsed record. Used to convert {@link Record} type to parsed record database table.
 * 
 * Enum string value must match those of {@link org.folio.rest.jaxrs.model.Record.RecordType}.
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
