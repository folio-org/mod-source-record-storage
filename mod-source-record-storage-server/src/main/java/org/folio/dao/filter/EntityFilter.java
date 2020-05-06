package org.folio.dao.filter;

/**
 * Interface to prepare WHERE clause for database lookups
 */
public interface EntityFilter {

  /**
   * Checks extended model for values to filter by and builds WHERE clause
   * 
   * @return WHERE clause for specific {@link EntityDAO} table
   */
  public String toWhereClause();

}