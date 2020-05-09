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
   * @param query    query dto which prepares WHERE AND ORDER BY clause for sql query
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with entity collection
   */
  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId);

  /**
   * Searches for entities by {@link EntityQuery} and stream results
   * 
   * @param query         query dto which prepares WHERE AND ORDER BY clause for sql query
   * @param offset        starting index in a list of results
   * @param limit         maximum number of results to return
   * @param tenantId      tenant id
   * @param entityHandler handler for entity stream
   * @param endHandler    handler for when stream is finished
   */
  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> entityHandler,
      Handler<AsyncResult<Void>> endHandler);

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
   * @param entities list of entities to save
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
   * Deletes entity from database
   * 
   * @param id       entity id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  public Future<Boolean> delete(String id, String tenantId);
  
}