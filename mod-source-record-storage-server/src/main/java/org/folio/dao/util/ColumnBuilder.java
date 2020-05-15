package org.folio.dao.util;

import static org.folio.dao.util.DaoUtil.COMMA;

/**
 * Simple column builder. Produces comma delimited list of column names.
 * e.g. id,content,description
 */
public class ColumnBuilder {

  private final StringBuilder columns;

  private ColumnBuilder() {
    columns = new StringBuilder();
  }

  private ColumnBuilder(String initialColumn) {
    this();
    columns.append(initialColumn);
  }

  public ColumnBuilder append(String column) {
    if (columns.length() > 0) {
      columns.append(COMMA);
    }
    columns.append(column);
    return this;
  }

  public String build() {
    return columns.toString();
  }

  public static ColumnBuilder of() {
    return new ColumnBuilder();
  }

  public static ColumnBuilder of(String initialColumn) {
    return new ColumnBuilder(initialColumn);
  }

}