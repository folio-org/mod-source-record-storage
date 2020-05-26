package org.folio.dao;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.folio.dao.query.EntityQuery;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;

/**
 * Data access object interface for <E> Entity with <C> Collection and <Q> {@link EntityQuery}
 */
public interface EntityDao<E, C, Q extends EntityQuery<Q>> {

  /**
   * Ability to execute action within transaction
   * 
   * @param tenantId tenant id
   * @param action   action
   * @return future with <T>
   */
  <T> Future<T> inTransaction(String tenantId, Function<SqlConnection, Future<T>> action);

  /**
   * Searches for entity by id
   * 
   * @param id       entity id
   * @param tenantId tenant id
   * @return future with optional entity
   */
  Future<Optional<E>> getById(String id, String tenantId);

  /**
   * Searches for entity by id
   *
   * @param connection connection
   * @param id         entity id
   * @param tenantId   tenant id
   * @return future with optional entity
   */
  Future<Optional<E>> getById(SqlConnection connection, String id, String tenantId);

  /**
   * Searches for entities by {@link EntityQuery}
   * 
   * @param query    {@link EntityQuery} which prepares WHERE and ORDER BY clauses for SQL template
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with entity collection
   */
  Future<C> getByQuery(Q query, int offset, int limit, String tenantId);

  /**
   * Searches for entities by {@link EntityQuery}
   * 
   * @param connection connection
   * @param query    {@link EntityQuery} which prepares WHERE and ORDER BY clauses for SQL template
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @param tenantId tenant id
   * @return future with entity collection
   */
  Future<C> getByQuery(SqlConnection connection, Q query, int offset, int limit, String tenantId);

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
  void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> entityHandler, Handler<AsyncResult<Void>> endHandler);

  /**
   * Saves entity to database
   * 
   * @param entity   entity to save
   * @param tenantId tenant id
   * @return future with saved entity
   */
  Future<E> save(E entity, String tenantId);

  /**
   * Saves entity to database
   * 
   * @param connection connection
   * @param entity     entity to save
   * @param tenantId   tenant id
   * @return future with saved entity
   */
  Future<E> save(SqlConnection connection, E entity, String tenantId);

  /**
   * Saves batch of entities to database
   * 
   * @param entities batch of entities to save
   * @param tenantId tenant id
   * @return future with list of saved entities
   */
  Future<List<E>> save(List<E> entities, String tenantId);

  /**
   * Saves batch of entities to database
   * 
   * @param connection connection
   * @param entities batch of entities to save
   * @param tenantId tenant id
   * @return future with list of saved entities
   */
  Future<List<E>> save(SqlConnection connection, List<E> entities, String tenantId);

  /**
   * Updates entity in database
   * 
   * @param entity   entity to update
   * @param tenantId tenant id
   * @return future with updated entity
   */
  Future<E> update(E entity, String tenantId);

  /**
   * Updates entity in database
   * 
   * @param connection connection
   * @param entity     entity to update
   * @param tenantId   tenant id
   * @return future with updated entity
   */
  Future<E> update(SqlConnection connection, E entity, String tenantId);

  /**
   * Deletes entity with id from database
   * 
   * @param id       entity id
   * @param tenantId tenant id
   * @return future with true if succeeded, else false
   */
  Future<Boolean> delete(String id, String tenantId);

  /**
   * Deletes entity with id from database
   * 
   * @param connection connection
   * @param id         entity id
   * @param tenantId   tenant id
   * @return future with true if succeeded, else false
   */
  Future<Boolean> delete(SqlConnection connection, String id, String tenantId);

  /**
   * Deletes entities by {@link EntityQuery} from database
   * 
   * @param query    entity query
   * @param tenantId tenant id
   * @return future with number of entities deleted
   */
  Future<Integer> delete(Q query, String tenantId);

  /**
   * Deletes entities by {@link EntityQuery} from database
   * 
   * @param connection connection
   * @param query    entity query
   * @param tenantId tenant id
   * @return future with number of entities deleted
   */
  Future<Integer> delete(SqlConnection connection, Q query, String tenantId);

  /**
   * Get table name for DAO
   * 
   * @return database table name for entity
   */
  String getTableName();

  /**
   * Prepare columns list for SELECT, INSERT and UPDATE queries
   * 
   * @return comma seperated list of table column names
   */
  String getColumns();

  /**
   * Get entity id
   * 
   * @param entity entity to retrieve id from
   * @return id of given entity
   */
  String getId(E entity);

}