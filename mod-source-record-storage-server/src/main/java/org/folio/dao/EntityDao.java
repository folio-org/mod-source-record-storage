package org.folio.dao;

import java.util.List;
import java.util.Optional;

import org.folio.dao.filter.EntityFilter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Data access object interface for Entity with Collection and {@link EntityFilter}
 */
public interface EntityDao<E, C, F extends EntityFilter> {

  /**
   * Searches for Entity by id
   * 
   * @param id       Entity id
   * @param tenantId tenant id
   * @return future with optional entity
   */
  public Future<Optional<E>> getById(String id, String tenantId);

  /**
   * Searchs for Entity by filter
   * 
   * @param filter   Entity Filter which prepares WHERE clause for query
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with entity collection
   */
  public Future<C> getByFilter(F filter, int offset, int limit, String tenantId);

  /**
   * Searchs for Entity by filter and stream results
   * 
   * @param filter       Entity Filter which prepares WHERE clause for query
   * @param offset       starting index in a list of results
   * @param limit        maximum number of results to return
   * @param tenantId     tenant id
   * @param handler      handler for Entity stream
   * @param replyHandler handler for when stream is finished
   */
  public void getByFilter(F filter, int offset, int limit, String tenantId, Handler<E> handler, Handler<AsyncResult<Void>> replyHandler);

  /**
   * Saves Entity to database
   * 
   * @param entity   Entity to save
   * @param tenantId tenant id
   * @return future with saved entity
   */
  public Future<E> save(E entity, String tenantId);

  /**
   * Saves batch of Entity to database
   * 
   * @param entities List of Entities to save
   * @param tenantId tenant id
   * @return future with list of saved entities
   */
  public Future<List<E>> save(List<E> entities, String tenantId);

  /**
   * Updates Entity in database
   * 
   * @param entity   Entity to save
   * @param tenantId tenant id
   * @return future with updated entity
   */
  public Future<E> update(E entity, String tenantId);

  /**
   * Deletes Entity from database
   * 
   * @param id       Entity id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  public Future<Boolean> delete(String id, String tenantId);

  /**
   * Get table name for DAO
   * 
   * @return database table name for entity
   */
  public String getTableName();

  /**
   * Prepare columns list for SELECT, INSERT and UPDATE queries
   * 
   * @return comma seperated list of table column names
   */
  public String getColumns();

  /**
   * Get Entity id
   * 
   * @param entity Entity to retrieve id from
   * @return id of given entity
   */
  public String getId(E entity);

}