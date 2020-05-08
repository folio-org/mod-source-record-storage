package org.folio.dao;

import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.DELETE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_FILTER_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_ID_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.SAVE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.UPDATE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.VALUE_TEMPLATE_TEMPLATE;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.ws.rs.NotFoundException;

import org.folio.dao.query.EntityQuery;
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
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;

public abstract class AbstractEntityDao<E, C, Q extends EntityQuery> implements EntityDao<E, C, Q> {

  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  @Autowired
  protected PostgresClientFactory postgresClientFactory;

  @Override
  public Future<Optional<E>> getById(String id, String tenantId) {
    String sql = String.format(GET_BY_ID_SQL_TEMPLATE, getColumns(), getTableName(), id);
    log.info("Attempting get by id: {}", sql);
    return select(sql, tenantId);
  }

  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String where = query.toWhereClause();
    String orderBy = query.toOrderByClause();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getColumns(), getTableName(), where, orderBy, offset, limit);
    log.info("Attempting get by filter: {}", sql);
    postgresClientFactory.getClient(tenantId).query(sql).execute(promise);
    return promise.future().map(this::toCollection);
  }

  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> handler, Handler<AsyncResult<Void>> endHandler) {
    String where = query.toWhereClause();
    String orderBy = query.toOrderByClause();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getColumns(), getTableName(), where, orderBy, offset, limit);
    log.info("Attempting stream get by filter: {}", sql);
    postgresClientFactory.getClient(tenantId).getConnection(ar1 -> {
      if (ar1.failed()) {
        log.error("Failed to get database connection", ar1.cause());
        endHandler.handle(Future.failedFuture(ar1.cause()));
        return;
      }
      SqlConnection connection = ar1.result();
      connection.prepare(sql, ar2 -> {
        if (ar2.failed()) {
          log.error("Failed to prepare query", ar2.cause());
          endHandler.handle(Future.failedFuture(ar2.cause()));
          return;
        }
        PreparedStatement pq = ar2.result();
        Transaction tx = connection.begin();
        RowStream<Row> stream = pq.createStream(limit, Tuple.tuple());
        stream
          .handler(row -> handler.handle(toEntity(row)))
          .exceptionHandler(e -> endHandler.handle(Future.failedFuture(e)))
          .endHandler(x -> {
            tx.commit();
            endHandler.handle(Future.succeededFuture());
          });
      });
    });
  }

  public Future<E> save(E entity, String tenantId) {
    Promise<E> promise = Promise.promise();
    String table = getTableName();
    String columns = getColumns();
    String valuesTemplate = getValuesTemplate(columns);
    String sqlTemplate = String.format(SAVE_SQL_TEMPLATE, table, columns, valuesTemplate);
    log.info("Attempting save: {}", sqlTemplate);
    postgresClientFactory.getClient(tenantId)
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

  public Future<List<E>> save(List<E> entities, String tenantId) {
    Promise<List<E>> promise = Promise.promise();
    log.info("Attempting batch save in {}", getTableName());
    String table = getTableName();
    String columns = getColumns();
    String valuesTemplate = getValuesTemplate(columns);
    String sqlTemplate = String.format(SAVE_SQL_TEMPLATE, table, columns, valuesTemplate);
    postgresClientFactory.getClient(tenantId)
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

  public Future<E> update(E entity, String tenantId) {
    Promise<E> promise = Promise.promise();
    String id = getId(entity);
    String table = getTableName();
    String columns = getColumns();
    String values = getValuesTemplate(columns);
    String sqlTemplate = String.format(UPDATE_SQL_TEMPLATE, table, columns, values, id);
    log.info("Attempting update: {}", sqlTemplate);
    postgresClientFactory.getClient(tenantId)
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

  public Future<Boolean> delete(String id, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String sql = String.format(DELETE_SQL_TEMPLATE, getTableName(), id);
    log.info("Attempting delete: {}", sql);
    postgresClientFactory.getClient(tenantId).query(sql).execute(promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  /**
   * Prepare list of tuples for multi-row INSERT and UPDATE query values
   * 
   * @param entities              list of Entities for extracting values for params
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return {@link List} of {@link Tuple} params
   */
  protected List<Tuple> toTuples(List<E> entities, boolean generateIdIfNotExists) {
    return entities.stream().map(entity -> toTuple(entity, generateIdIfNotExists)).collect(Collectors.toList());
  }

  /**
   * Convert {@link RowSet} into Entity
   * 
   * @param resultSet {@link RowSet} query results
   * @return optional Entity
   */
  protected Optional<E> toEntity(RowSet<Row> rowSet)  {
    return rowSet.rowCount() > 0 ? Optional.of(toEntity(rowSet.iterator().next())) : Optional.empty();
  }

  /**
   * Prepare values list for INSERT and UPDATE queries
   * 
   * @param columns comma seperated list of column names
   * @return comma seperated list of question marks matching number of columns
   */
  protected String getValuesTemplate(String columns) {
    return IntStream.range(1, columns.split(COMMA).length + 1)
      .mapToObj(Integer::toString)
      .map(i -> String.format(VALUE_TEMPLATE_TEMPLATE, i))
      .collect(Collectors.joining(COMMA));
  }

  /**
   * Submit SQL query
   * 
   * @param sql      SQL query
   * @param tenantId tenant id
   * @return future of optional Entity
   */
  protected Future<Optional<E>> select(String sql, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    postgresClientFactory.getClient(tenantId).query(sql).execute(promise);
    return promise.future().map(this::toEntity);
  }

  /**
   * Post save processing of Entity
   * 
   * @param entity saved Entity
   * @return entity after post save processing
   */
  protected E postSave(E entity) {
    return entity;
  }

  /**
   * Post update processing of Entity
   * 
   * @param entity updated Entity
   * @return entity after post update processing
   */
  protected E postUpdate(E entity) {
    return entity;
  }

  /**
   * Post save processing of list of Entities
   * 
   * @param entities saved list of Entities
   * @return entity after post save processing
   */
  protected List<E> postSave(List<E> entities) {
    return entities.stream().map(this::postSave).collect(Collectors.toList());
  }

  /**
   * Prepare tuple for INSERT and UPDATE query values. Must be in same order as columns.
   * 
   * @param entity                Entity for extracting values for params
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return {@link Tuple} tuple
   */
  protected abstract Tuple toTuple(E entity, boolean generateIdIfNotExists);

  /**
   * Convert {@link RowSet} into Entity Collection
   * 
   * @param resultSet {@link RowSet} query results
   * @return Entity Collection
   */
  protected abstract C toCollection(RowSet<Row> resultSet);

  /**
   * Convert {@link Row} into Entity
   * 
   * @param result {@link Row} query result row
   * @return Entity
   */
  protected abstract E toEntity(Row row);

}