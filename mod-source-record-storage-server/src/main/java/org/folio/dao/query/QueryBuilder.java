package org.folio.dao.query;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.DATE_FORMATTER;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.folio.dao.query.OrderBy.Direction;

/**
 * 
 */
public class QueryBuilder {

  public static final String ORDER_BY_TEMPLATE = "ORDER BY %s";
  public static final String WHERE_TEMPLATE = "WHERE %s";

  public static final String SPACE = " ";
  public static final String DOUBLE_SPACE = "  ";

  private final EntityQuery query;

  private final List<Where> filter;

  private final List<OrderBy> sort;

  private QueryBuilder(EntityQuery query) {
    this.query = query;
    this.filter = new ArrayList<>();
    this.sort = new ArrayList<>();
  }

  /**
   * Build SQL WHERE clause from list of {@link Where}
   * 
   * @return SQL WHERE clause
   */
  public String buildWhereClause() {
    StringBuilder whereClause =  new StringBuilder();
    for (Where where : filter) {
      switch(where.getOp()) {
        case AND:
        case OR:
        case START_EXPRESSION:
        case END_EXPRESSION:
          whereClause.append(String.format("%s ", where.getOp().getToken()));
          break;
        default:
          Optional<String> column = query.propertyColumnName(where.getProperty());
          if (column.isPresent()) {
            whereClause.append(String.format("%s %s %s ", column.get(),
              where.getOp().getToken(), getValue(where)));
          } else {
            throw new BadRequestException(String.format("%s cannot be mapped to a column",
              where.getProperty()));
          }
          break;
      }
    }
    return whereClause.length() > 0
      ? String.format(WHERE_TEMPLATE, whereClause.toString().replace(DOUBLE_SPACE, SPACE)).trim()
      : EMPTY;
  }

  /**
   * Build SQL ORDER BY clasue from list of {@link OrderBy}
   * 
   * @return SQL ORDER BY clause
   */
  public String buildOrderByClause() {
    StringBuilder oderByClause =  new StringBuilder();
    for (OrderBy orderBy : sort) {
      Optional<String> column = query.propertyColumnName(orderBy.getProperty());
      if (column.isPresent()) {
        if (oderByClause.length() > 0) {
          oderByClause
            .append(COMMA)
            .append(SPACE);
        }
        oderByClause
          .append(column.get())
          .append(SPACE)
          .append(orderBy.getDirection().toString());
      } else {
        throw new BadRequestException(String.format("%s cannot be mapped to a column",
          orderBy.getProperty()));
      }
    }
    return oderByClause.length() > 0
      ? String.format(ORDER_BY_TEMPLATE, oderByClause.toString())
      : EMPTY;
  }

  /**
   * Order by property default ascending
   * 
   * @param property
   * @return {@link EntityQuery} to allow fluent use
   */
  public QueryBuilder orderBy(String property) {
    sort.add(OrderBy.by(property));
    return this;
  }

  /**
   * Order by property by {@link Direction}
   * 
   * @param property  property of entity to order by
   * @param direction {@link Direction} of sort
   * @return {@link EntityQuery} to allow fluent use
   */
  public QueryBuilder orderBy(String property, Direction direction) {
    sort.add(OrderBy.by(property, direction));
    return this;
  }

  public QueryBuilder whereEqual(String property, Object value) {
    filter.add(Where.equal(property, value));
    return this;
  }

  public QueryBuilder whereGreaterThen(String property, Object value) {
    filter.add(Where.greaterThen(property, value));
    return this;
  }

  public QueryBuilder whereLessThen(String property, Object value) {
    filter.add(Where.lessThen(property, value));
    return this;
  }

  public QueryBuilder whereGreaterThenOrEqual(String property, Object value) {
    filter.add(Where.greaterThenOrEqual(property, value));
    return this;
  }

  public QueryBuilder whereLessThenOrEqual(String property, Object value) {
    filter.add(Where.lessThenOrEqual(property, value));
    return this;
  }

  public QueryBuilder whereNotEqual(String property, Object value) {
    filter.add(Where.notEqual(property, value));
    return this;
  }

  public QueryBuilder whereLike(String property, String value) {
    filter.add(Where.like(property, value));
    return this;
  }

  public <T> QueryBuilder whereBetween(String property, T from, T to) {
    filter.add(Where.between(property, from, to));
    return this;
  }

  public <T> QueryBuilder whereIn(String property, Collection<T> values) {
    filter.add(Where.in(property, values));
    return this;
  }

  public QueryBuilder and() {
    filter.add(Where.and());
    return this;
  }

  public QueryBuilder or() {
    filter.add(Where.or());
    return this;
  }

  public QueryBuilder startExpression() {
    filter.add(Where.startExpression());
    return this;
  }

  public QueryBuilder endExpression() {
    filter.add(Where.endExpression());
    return this;
  }

  public EntityQuery query() {
    return query;
  }

  private String getValue(Where where) {
    String property = where.getProperty();
    switch(where.getOp()) {
      case AND:
      case OR:
      case START_EXPRESSION:
      case END_EXPRESSION:
        // do nothing as shouldn't get here
        break;
      case BETWEEN:
        Object[] value = (Object[]) where.getValue();
        return String.format("%s AND %s ",
          getValue(property, value[0]),
          getValue(property, value[1]));
      case IN:
        Collection<?> values = (Collection<?>) where.getValue();
        return String.format("(%s) ", getListValue(property, values));
      default:
        return String.format("%s ", getValue(property, where.getValue()));
    }
    return EMPTY;
  }

  private String getValue(String property, Object value) {
    List<String> path = new ArrayList<>(Arrays.asList(property.split("\\.")));
    Field field = getField(query.queryFor(), path);
    Class<?> type = field.getType();
    if (String.class.isAssignableFrom(type) || type.isEnum()) {
      return String.format("'%s'", String.valueOf(value));
    } else if (Date.class.isAssignableFrom(type)) {
      return String.format("'%s'", DATE_FORMATTER.format((Date) value));
    } else {
      return String.valueOf(value);
    }
  }

  private String getListValue(String property, Collection<?> values) {
    return values.stream().map(value -> getValue(property, value)).collect(Collectors.joining(COMMA));
  }

  private Field getField(Class<?> clazz, List<String> path) {
    Field field = FieldUtils.getField(clazz, path.get(0), true);
    if (path.size() > 1) {
      return getField(field.getType(), path.subList(1, path.size()));
    }
    return field;
  }

  public static QueryBuilder builder(EntityQuery query) {
    return new QueryBuilder(query);
  }

}