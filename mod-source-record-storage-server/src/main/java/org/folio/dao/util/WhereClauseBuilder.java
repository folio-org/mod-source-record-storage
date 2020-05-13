package org.folio.dao.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.dao.util.DaoUtil.COLUMN_EQUALS_TEMPLATE;
import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;
import static org.folio.dao.util.DaoUtil.SPACED_AND;
import static org.folio.dao.util.DaoUtil.UNWRAPPED_TEMPLATE;
import static org.folio.dao.util.DaoUtil.WHERE_TEMPLATE;
import static org.folio.dao.util.DaoUtil.WRAPPED_TEMPLATE;

import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

public class WhereClauseBuilder {

  private final StringBuilder whereClause;

  private WhereClauseBuilder() {
    whereClause = new StringBuilder();
  }

  public WhereClauseBuilder append(Object value, String column) {
    return wrappedWithCheck(value, column);
  }

  public WhereClauseBuilder append(String value, String column) {
    if (StringUtils.isNotEmpty(value)) {
      return wrapped(value, column);
    }
    return this;
  }

  public WhereClauseBuilder append(Integer value, String column) {
    return unwrappedWithCheck(value, column);
  }

  public WhereClauseBuilder append(Boolean value, String column) {
    return unwrappedWithCheck(value, column);
  }

  public WhereClauseBuilder append(Date value, String column) {
    if (Objects.nonNull(value)) {
      return wrapped(DATE_FORMATTER.format(value), column);
    }
    return this;
  }

  public String build() {
    return whereClause.length() > 0
      ? String.format(WHERE_TEMPLATE, whereClause.toString())
      : EMPTY;
  }

  private WhereClauseBuilder wrappedWithCheck(Object value, String column) {
    if (Objects.nonNull(value)) {
      return wrapped(value, column);
    }
    return this;
  }

  private WhereClauseBuilder wrapped(Object value, String column) {
    if (whereClause.length() > 0) {
      whereClause.append(SPACED_AND);
    }
    whereClause
      .append(String.format(COLUMN_EQUALS_TEMPLATE, column))
      .append(String.format(WRAPPED_TEMPLATE, value));
    return this;
  }

  private WhereClauseBuilder unwrappedWithCheck(Object value, String column) {
    if (Objects.nonNull(value)) {
      return unwrapped(value, column);
    }
    return this;
  }

  private WhereClauseBuilder unwrapped(Object value, String column) {
    if (whereClause.length() > 0) {
      whereClause.append(SPACED_AND);
    }
    whereClause
      .append(String.format(COLUMN_EQUALS_TEMPLATE, column))
      .append(String.format(UNWRAPPED_TEMPLATE, value));
    return this;
  }

  public static WhereClauseBuilder of() {
    return new WhereClauseBuilder();
  }

}