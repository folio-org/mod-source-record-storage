package org.folio.dao;

import java.util.List;
import java.util.Optional;

import org.folio.dao.filter.BeanFilter;

import io.vertx.core.Future;

/**
 * Data access object interface for Bean with Collection and {@link BeanFilter}
 */
public interface BeanDao<B, C, F extends BeanFilter> {

  /**
   * Searches for Bean by id
   * 
   * @param id       Bean id
   * @param tenantId tenant id
   * @return future with optional bean
   */
  public Future<Optional<B>> getById(String id, String tenantId);

  /**
   * Searchs for Bean by filter
   * 
   * @param filter   Bean Filter which prepares WHERE clause for query
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with bean collection
   */
  public Future<C> getByFilter(F filter, int offset, int limit, String tenantId);

  /**
   * Saves Bean to database
   * 
   * @param bean     Bean to save
   * @param tenantId tenant id
   * @return future with saved bean
   */
  public Future<B> save(B bean, String tenantId);

  /**
   * Saves batch of Bean to database
   * 
   * @param beans    List of Beans to save
   * @param tenantId tenant id
   * @return future with list of saved beans
   */
  public Future<List<B>> save(List<B> beans, String tenantId);

  /**
   * Updates Bean in database
   * 
   * @param bean     Bean to save
   * @param tenantId tenant id
   * @return future with updated bean
   */
  public Future<B> update(B bean, String tenantId);

  /**
   * Deletes Bean from database
   * 
   * @param id       Bean id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  public Future<Boolean> delete(String id, String tenantId);

  /**
   * Get table name for DAO
   * 
   * @return database table name for bean
   */
  public String getTableName();

  /**
   * Get Bean id
   * 
   * @param bean     Bean to retrieve id from
   * @return id of given bean
   */
  public String getId(B bean);

}