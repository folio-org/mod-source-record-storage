package org.folio.dao;

import java.util.List;
import java.util.Optional;

import org.folio.dao.query.EntityQuery;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Data access object interface for <E> Entity with <C> Collection and <Q> {@link EntityQuery}
 */
public interface EntityDao<E, C, Q extends EntityQuery> {

  /**
   * Searches for entity by id
   * 
   * @param id       entity id
   * @param tenantId tenant id
   * @return future with optional entity
   */
  public Future<Optional<E>> getById(String id, String tenantId);

  /**
   * Searches for entities by {@link EntityQuery}
   * 
   * @param query    {@link EntityQuery} which prepares WHERE and ORDER BY clauses for SQL template
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with entity collection
   */
  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId);

  /**
   * Searches for entities by {@link EntityQuery} and stream results
   * 
   * @param query         {@link EntityQuery} which prepares WHERE and ORDER BY clauses for SQL template
   * @param offset        starting index in a list of results
   * @param limit         maximum number of results to return
   * @param tenantId      tenant id
   * @param entityHandler handler for stream of Entities
   * @param endHandler    handler for when stream is finished
   */
  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> entityHandler, Handler<AsyncResult<Void>> endHandler);

  /**
   * Saves entity to database
   * 
   * @param entity   entity to save
   * @param tenantId tenant id
   * @return future with saved entity
   */
  public Future<E> save(E entity, String tenantId);

  /**
   * Saves batch of entities to database
   * 
   * @param entities batch of entities to save
   * @param tenantId tenant id
   * @return future with list of saved entities
   */
  public Future<List<E>> save(List<E> entities, String tenantId);

  /**
   * Updates entity in database
   * 
   * @param entity   entity to update
   * @param tenantId tenant id
   * @return future with updated entity
   */
  public Future<E> update(E entity, String tenantId);

  /**
   * Deletes entity with id from database
   * 
   * @param id       entity id
   * @param tenantId tenant id
   * @return future with true if succeeded, else false
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
   * Get entity id
   * 
   * @param entity entity to retrieve id from
   * @return id of given entity
   */
  public String getId(E entity);

}