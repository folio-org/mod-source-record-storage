package org.folio.dao.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.ws.rs.BadRequestException;

import org.folio.dao.query.OrderBy.Direction;
import org.folio.dao.util.OrderByClauseBuilder;

/**
 * Interface to prepare WHERE and ORDER BY clauses for database lookups
 */
public interface EntityQuery {

  public final List<OrderBy> orders = new ArrayList<>();

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
  public default String toOrderByClause() throws BadRequestException {
    OrderByClauseBuilder orderByClauseBuilder = OrderByClauseBuilder.of();
    for (OrderBy orderBy : orders) {
      Optional<String> column = getPropertyColumnName(orderBy.getProperty());
      if (column.isPresent()) {
        orderByClauseBuilder.add(column.get(), orderBy.getDirection());
      } else {
        throw new BadRequestException(String.format("%s cannot be mapped to a column",
          orderBy.getProperty()));
      }
    }
    return orderByClauseBuilder.build();
  }

  /**
   * Lookup table column name for given property
   * 
   * @return {@link Optional} of {@link String} column name for given property
   */
  public Optional<String> getPropertyColumnName(String property);

  /**
   * Order by property default ascending
   * 
   * @param property
   * @return {@link EntityQuery} to allow fluent use
   */
  public default EntityQuery orderBy(String property) {
    orders.add(OrderBy.of(property));
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
    orders.add(OrderBy.of(property, direction));
    return this;
  }

}