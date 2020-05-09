package org.folio.services;

import java.util.List;
import java.util.Optional;

import org.folio.dao.query.EntityQuery;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * Entity service interface for <E> Entity with <C> Collection and <Q> {@link EntityQuery}
 */
public interface EntityService<E, C, Q extends EntityQuery> {

  /**
   * Searches for Entity by id
   * 
   * @param id       Entity id
   * @param tenantId tenant id
   * @return future with optional entity
   */
  public Future<Optional<E>> getById(String id, String tenantId);

  /**
   * Searches for Entity by query
   * 
   * @param query    {@link EntityQuery} which prepares WHERE clause for query
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with entity collection
   */
  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId);

  /**
   * Searches for Entity by query and stream results
   * 
   * @param query        {@link EntityQuery} which prepares WHERE clause for query
   * @param offset       starting index in a list of results
   * @param limit        maximum number of results to return
   * @param tenantId     tenant id
   * @param handler      handler for Entity stream
   * @param replyHandler handler for when stream is finished
   */
  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> handler, Handler<AsyncResult<Void>> replyHandler);

  /**
   * Saves Entity to database
   * 
   * @param entity   Entity to save
   * @param tenantId tenant id
   * @return future with saved entity
   */
  public Future<E> save(E entity, String tenantId);

  /**
   * Saves batch of Entities to database
   * 
   * @param entities {@link List} of Entities to save
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
  
}