package org.folio.dao.query;

import java.util.Map;
import java.util.Optional;

/**
 * Interface to prepare WHERE and ORDER BY clauses for database sql queries
 */
public interface EntityQuery {

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
  public default Optional<String> propertyColumnName(String property) {
    return Optional.ofNullable(getPropertyToColumn().get(property));
  }

   /**
   * Get class of object represented by table
   * 
   * @return class
   */
  public Class<?> queryFor();

  /**
   * Get {@link QueryBuilder}
   * 
   * @return query builder for this entity query
   */
  public QueryBuilder builder();

  /**
   * Builds and returns WHERE clause
   * 
   * @return SQL WHERE clause
   */
  public default String toWhereClause() {
    return builder().buildWhereClause();
  }

  /**
   * Builds and returns ORDER BY clause
   * 
   * @return SQL ORDER BY clause
   */
  public default String toOrderByClause() {
    return builder().buildOrderByClause();
  }

}