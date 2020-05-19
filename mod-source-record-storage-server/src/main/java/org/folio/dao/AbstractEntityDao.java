package org.folio.dao;

import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.DELETE_BY_ID_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.DELETE_BY_QUERY_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_ID_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_QUERY_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_QUERY_WITH_TOTAL_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.SAVE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.UPDATE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.VALUE_TEMPLATE_TEMPLATE;
import static org.folio.dao.util.DaoUtil.execute;
import static org.folio.dao.util.DaoUtil.executeInTransaction;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.ws.rs.NotFoundException;

import org.folio.dao.query.EntityQuery;
import org.folio.dao.util.DaoUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

public abstract class AbstractEntityDao<E, C, Q extends EntityQuery<Q>> implements EntityDao<E, C, Q> {

  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  @Autowired
  protected PostgresClientFactory postgresClientFactory;

  @Override
  public Future<Optional<E>> getById(String id, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      getById(connection, id, tenantId));
  }

  @Override
  public Future<Optional<E>> getById(SqlConnection connection, String id, String tenantId) {
    String sql = String.format(GET_BY_ID_SQL_TEMPLATE, getColumns(), getTableName(), id);
    log.info("Attempting get by id: {}", sql);
    return select(connection, sql, tenantId);
  }

  @Override
  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      getByQuery(connection, query, offset, limit, tenantId));
  }

  @Override
  public Future<C> getByQuery(SqlConnection connection, Q query, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String where = query.getWhereClause();
    String orderBy = query.getOrderByClause();
    String sql = String.format(GET_BY_QUERY_WITH_TOTAL_SQL_TEMPLATE, getColumns(), getTableName(), where, orderBy, offset, limit);
    log.info("Attempting get by query: {}", sql);
    connection.query(sql).execute(promise);
    return promise.future().map(resultSet -> DaoUtil.hasRecords(resultSet)
      ? toCollection(resultSet)
      : toEmptyCollection(resultSet));
  }

  @Override
  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> entityHandler, Handler<AsyncResult<Void>> endHandler) {
    String where = query.getWhereClause();
    String orderBy = query.getOrderByClause();
    String sql = String.format(GET_BY_QUERY_SQL_TEMPLATE, getColumns(), getTableName(), where, orderBy, offset, limit);
    log.info("Attempting stream get by filter: {}", sql);
    executeInTransaction(postgresClientFactory.getClient(tenantId), connection -> {
      Promise<Void> promise = Promise.promise();
      connection.prepare(sql, ar2 -> {
        if (ar2.failed()) {
          log.error("Failed to prepare query", ar2.cause());
          endHandler.handle(Future.failedFuture(ar2.cause()));
          return;
        }
        PreparedStatement pq = ar2.result();
        RowStream<Row> stream = pq.createStream(limit, Tuple.tuple());
        stream
          .handler(row -> entityHandler.handle(toEntity(row)))
          .exceptionHandler(e -> endHandler.handle(Future.failedFuture(e)))
          .endHandler(e -> { 
            endHandler.handle(Future.succeededFuture());
            promise.complete();
          });
      });
      return promise.future();
    });
  }

  @Override
  public Future<E> save(E entity, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      save(connection, entity, tenantId));
  }

  @Override
  public Future<E> save(SqlConnection connection, E entity, String tenantId) {
    Promise<E> promise = Promise.promise();
    String table = getTableName();
    String columns = getColumns();
    String valuesTemplate = getValuesTemplate(columns);
    String sqlTemplate = String.format(SAVE_SQL_TEMPLATE, table, columns, valuesTemplate);
    log.info("Attempting save: {}", sqlTemplate);
    connection
      .preparedQuery(sqlTemplate)
      .execute(toTuple(entity, true), save -> {
        if (save.failed()) {
          log.error("Failed to insert row in {}", save.cause(), getTableName());
          promise.fail(save.cause());
          return;
        }
        promise.complete(postSave(entity));
      });
    return promise.future();
  }

  @Override
  public Future<List<E>> save(List<E> entities, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      save(connection, entities, tenantId));
  }

  @Override
  public Future<List<E>> save(SqlConnection connection, List<E> entities, String tenantId) {
    Promise<List<E>> promise = Promise.promise();
    log.info("Attempting batch save in {}", getTableName());
    String table = getTableName();
    String columns = getColumns();
    String valuesTemplate = getValuesTemplate(columns);
    String sqlTemplate = String.format(SAVE_SQL_TEMPLATE, table, columns, valuesTemplate);
    connection
      .preparedQuery(sqlTemplate)
      .executeBatch(toTuples(entities, true), batch -> {
        if (batch.failed()) {
          log.error("Failed to insert multiple rows in {}", batch.cause(), getTableName());
          promise.fail(batch.cause());
          return;
        }
        promise.complete(postSave(entities));
      });
    return promise.future();
  }

  @Override
  public Future<E> update(E entity, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      update(connection, entity, tenantId));
  }

  @Override
  public Future<E> update(SqlConnection connection, E entity, String tenantId) {
    Promise<E> promise = Promise.promise();
    String id = getId(entity);
    String table = getTableName();
    String columns = getColumns();
    String valuesTemplate = getValuesTemplate(columns);
    String sqlTemplate = String.format(UPDATE_SQL_TEMPLATE, table, columns, valuesTemplate, id);
    log.info("Attempting update: {}", sqlTemplate);
    connection
      .preparedQuery(sqlTemplate)
      .execute(toTuple(entity, false), update -> {
        if (update.failed()) {
          log.error("Failed to update row in {} with id {}", update.cause(), getTableName(), id);
          promise.fail(update.cause());
          return;
        }
        if (update.result().rowCount() == 0) {
          promise.fail(new NotFoundException(String.format("%s row with id %s was not updated", getTableName(), id)));
          return;
        }
        promise.complete(postUpdate(entity));
      });
    return promise.future();
  }

  @Override
  public Future<Boolean> delete(String id, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      delete(connection, id, tenantId));
  }

  @Override
  public Future<Boolean> delete(SqlConnection connection, String id, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String sql = String.format(DELETE_BY_ID_SQL_TEMPLATE, getTableName(), id);
    log.info("Attempting delete by id: {}", sql);
    connection.query(sql).execute(promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  @Override
  public Future<Integer> delete(Q query, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      delete(connection, query, tenantId));
  }

  @Override
  public Future<Integer> delete(SqlConnection connection, Q query, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String sql = String.format(DELETE_BY_QUERY_SQL_TEMPLATE, getTableName(), query.getWhereClause());
    log.info("Attempting delete by query: {}", sql);
    connection.query(sql).execute(promise);
    return promise.future().map(this::toRowCount);
  }

  /**
   * Submit SQL select query which returns result mapping to an entity
   * 
   * @param sql      SQL query
   * @param tenantId tenant id
   * @return future of optional entity
   */
  protected Future<Optional<E>> select(String sql, String tenantId) {
    return execute(postgresClientFactory.getClient(tenantId), connection ->
      select(connection, sql, tenantId));
  }

  /**
   * Submit SQL select query which returns result mapping to an entity
   * 
   * @param connection connection
   * @param sql        SQL query
   * @param tenantId   tenant id
   * @return future of optional entity
   */
  protected Future<Optional<E>> select(SqlConnection connection, String sql, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    connection.query(sql).execute(promise);
    return promise.future().map(this::toEntity);
  }

  /**
   * Prepare list of tuples for multi-row INSERT and UPDATE query values
   * 
   * @param entities              list of entities for extracting values for SQL template parameters
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return list of tuple as sql template parameters
   */
  protected List<Tuple> toTuples(List<E> entities, boolean generateIdIfNotExists) {
    return entities.stream().map(entity -> toTuple(entity, generateIdIfNotExists)).collect(Collectors.toList());
  }

  /**
   * Convert {@link RowSet} into entity
   * 
   * @param rowSet {@link RowSet} query results
   * @return optional entity
   */
  protected Optional<E> toEntity(RowSet<Row> rowSet)  {
    return rowSet.rowCount() > 0 ? Optional.of(toEntity(rowSet.iterator().next())) : Optional.empty();
  }

  /**
   * Prepare values template for INSERT and UPDATE queries
   * 
   * @param columns comma seperated list of column names
   * @return comma seperated list of numbered template tokens
   */
  protected String getValuesTemplate(String columns) {
    return IntStream.range(1, columns.split(COMMA).length + 1)
      .mapToObj(Integer::toString)
      .map(i -> String.format(VALUE_TEMPLATE_TEMPLATE, i))
      .collect(Collectors.joining(COMMA));
  }

  /**
   * Post save processing of entity
   * 
   * @param entity saved entity
   * @return entity after post save processing
   */
  protected E postSave(E entity) {
    return entity;
  }

  /**
   * Post update processing of entity
   * 
   * @param entity updated entity
   * @return entity after post update processing
   */
  protected E postUpdate(E entity) {
    return entity;
  }

  /**
   * Post save processing of list of entities. Do nothing by default.
   * 
   * @param entities saved list of entities
   * @return entity after post save processing
   */
  protected List<E> postSave(List<E> entities) {
    return entities;
  }

  /**
   * Prepare {@link Tuple} for INSERT and UPDATE query values. Must be in same order as columns.
   * 
   * @param entity                entity for extracting values for SQL template parameters
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return tuple for sql template paramters
   */
  protected abstract Tuple toTuple(E entity, boolean generateIdIfNotExists);

  /**
   * Convert {@link RowSet} into Entity Collection
   * 
   * @param rowSet {@link RowSet} query results
   * @return Entity Collection
   */
  protected abstract C toCollection(RowSet<Row> rowSet);

  /**
   * Convert {@link RowSet} into an empty Entity Collection
   * 
   * @param rowSet {@link RowSet} query results
   * @return Entity Collection with total results only
   */
  protected abstract C toEmptyCollection(RowSet<Row> rowSet);

  /**
   * Convert {@link Row} into Entity
   * 
   * @param result query result row
   * @return entity mapped from row
   */
  protected abstract E toEntity(Row row);

  private Integer toRowCount(RowSet<Row> rowSet) {
    return rowSet.rowCount();
  }

}