package org.folio.dao.filter;

/**
 * Interface to prepare WHERE clause for database lookups
 */
public interface BeanFilter {

  /**
   * Checks extended model for values to filter by and builds WHERE clause
   * 
   * @return WHERE clause for specific {@link BeanDAO} table
   */
  public String toWhereClause();

}