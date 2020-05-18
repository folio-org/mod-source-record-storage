package org.folio.dao.query;

/**
 * Interface to prepare WHERE and ORDER BY clauses for database sql queries
 */
public interface EntityQuery {

  /**
   * Get {@link QueryBuilder}
   * 
   * @return query builder for this entity query
   */
  public QueryBuilder<?> builder();

  /**
   * Builds and returns WHERE clause
   * 
   * @return SQL WHERE clause
   */
  public default String getWhereClause() {
    return builder().buildWhereClause();
  }

  /**
   * Builds and returns ORDER BY clause
   * 
   * @return SQL ORDER BY clause
   */
  public default String getOrderByClause() {
    return builder().buildOrderByClause();
  }

}