package org.folio.dao.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.ORDER_BY_TEMPLATE;
import static org.folio.dao.util.DaoUtil.SPACE;

import org.folio.dao.query.OrderBy.Direction;

public class OrderByClauseBuilder {

  private final StringBuilder oderByClause;

  private OrderByClauseBuilder() {
    oderByClause = new StringBuilder();
  }

  public OrderByClauseBuilder add(String column, Direction direction) {
    if (oderByClause.length() > 0) {
      oderByClause.append(COMMA);
    }
    oderByClause.append(column)
      .append(SPACE)
      .append(direction.toString());
    return this;
  }

  public String build() {
    return oderByClause.length() > 0
      ? String.format(ORDER_BY_TEMPLATE, oderByClause.toString())
      : EMPTY;
  }

  public static OrderByClauseBuilder of() {
    return new OrderByClauseBuilder();
  }

}