package org.folio.dao.query;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.ORDER_BY_TEMPLATE;
import static org.folio.dao.util.DaoUtil.SPACE;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.ws.rs.BadRequestException;

import org.folio.dao.query.OrderBy.Direction;

/**
 * Interface to prepare WHERE and ORDER BY clauses for database sql queries
 */
public interface EntityQuery {

  /**
   * Get sort orders
   * 
   * @return {@link Set} of {@link OrderBy}
   */
  public Set<OrderBy> getSort();

  /**
   * Get property to column name map
   * 
   * @return property to column name map
   */
  public Map<String, String> getPropertyToColumn();

  /**
   * Lookup table column name for given property
   * 
   * @return column name for given property
   */
  public default Optional<String> getPropertyColumnName(String property) {
    return Optional.ofNullable(getPropertyToColumn().get(property));
  }

  /**
   * Checks extended model for values to query by and builds WHERE clause
   * 
   * @return WHERE clause for specific {@link EntityDAO} table
   */
  public String toWhereClause();

  /**
   * Builds ORDER BY clause if query specifies any {@link OrderBy} in orders
   * 
   * @return ORDER BY claues for specific {@link EntityDAO} table
   * @throws BadRequestException
   */
  public default String toOrderByClause() {
    StringBuilder oderByClause =  new StringBuilder();
    for (OrderBy orderBy : getSort()) {
      Optional<String> column = getPropertyColumnName(orderBy.getProperty());
      if (column.isPresent()) {
        if (oderByClause.length() > 0) {
          oderByClause.append(COMMA);
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
  public default EntityQuery orderBy(String property) {
    getSort().add(OrderBy.of(property));
    return this;
  }

  /**
   * Order by property by {@link Direction}
   * 
   * @param property  property of entity to order by
   * @param direction {@link Direction} of sort
   * @return {@link EntityQuery} to allow fluent use
   */
  public default EntityQuery orderBy(String property, Direction direction) {
    getSort().add(OrderBy.of(property, direction));
    return this;
  }

}