package org.folio.dao.util;

import static org.folio.dao.util.DaoUtil.COMMA;

import org.apache.commons.lang3.StringUtils;

public class ColumnsBuilder {

  private final StringBuilder columns;

  private ColumnsBuilder() {
    columns = new StringBuilder();
  }

  private ColumnsBuilder(String initialColumn) {
    this();
    columns.append(initialColumn);
  }

  public ColumnsBuilder append(Object value, String column) {
    if (value != null) {
      append(column);
    }
    return this;
  }

  public ColumnsBuilder append(String value, String column) {
    if (StringUtils.isNotEmpty(value)) {
      append(column);
    }
    return this;
  }

  private ColumnsBuilder append(String column) {
    if (columns.length() > 0) {
      columns.append(COMMA);
    }
    columns.append(column);
    return this;
  }

  public String build() {
    return columns.toString();
  }

  public static ColumnsBuilder of() {
    return new ColumnsBuilder();
  }

  public static ColumnsBuilder of(String initialColumn) {
    return new ColumnsBuilder(initialColumn);
  }

}