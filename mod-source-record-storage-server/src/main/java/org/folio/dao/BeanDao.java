package org.folio.dao;

import java.util.List;
import java.util.Optional;

import org.folio.dao.filter.BeanFilter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Data access object interface for Bean with Collection and {@link BeanFilter}
 */
public interface BeanDao<I, C, F extends BeanFilter> {

  /**
   * Searches for Bean by id
   * 
   * @param id       Bean id
   * @param tenantId tenant id
   * @return future with optional bean
   */
  public Future<Optional<I>> getById(String id, String tenantId);

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
   * Searchs for Bean by filter and stream results
   * 
   * @param filter       Bean Filter which prepares WHERE clause for query
   * @param offset       starting index in a list of results
   * @param limit        maximum number of results to return
   * @param tenantId     tenant id
   * @param handler      handler for each Bean
   * @param replyHandler handler for when stream is finished
   */
  public void getByFilter(F filter, int offset, int limit, String tenantId, Handler<I> handler, Handler<AsyncResult<Void>> replyHandler);

  /**
   * Saves Bean to database
   * 
   * @param bean     Bean to save
   * @param tenantId tenant id
   * @return future with saved bean
   */
  public Future<I> save(I bean, String tenantId);

  /**
   * Saves batch of Bean to database
   * 
   * @param beans    List of Beans to save
   * @param tenantId tenant id
   * @return future with list of saved beans
   */
  public Future<List<I>> save(List<I> beans, String tenantId);

  /**
   * Updates Bean in database
   * 
   * @param bean     Bean to save
   * @param tenantId tenant id
   * @return future with updated bean
   */
  public Future<I> update(I bean, String tenantId);

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
   * Prepare columns list for SELECT, INSERT and UPDATE queries
   * 
   * @return comma seperated list of table column names
   */
  public String getColumns();

  /**
   * Get Bean id
   * 
   * @param bean     Bean to retrieve id from
   * @return id of given bean
   */
  public String getId(I bean);

}