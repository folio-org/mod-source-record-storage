package org.folio.dao;

import static org.folio.dao.util.DaoUtil.COMMA;
import static org.folio.dao.util.DaoUtil.DELETE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_FILTER_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.GET_BY_ID_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.QUESTION_MARK;
import static org.folio.dao.util.DaoUtil.SAVE_SQL_TEMPLATE;
import static org.folio.dao.util.DaoUtil.UPDATE_SQL_TEMPLATE;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.folio.dao.query.EntityQuery;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;

public abstract class AbstractEntityDao<E, C, Q extends EntityQuery> implements EntityDao<E, C, Q> {

  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  @Autowired
  protected PostgresClientFactory postgresClientFactory;

  public Future<Optional<E>> getById(String id, String tenantId) {
    String sql = String.format(GET_BY_ID_SQL_TEMPLATE, getColumns(), getTableName(), id);
    log.info("Attempting get by id: {}", sql);
    return select(sql, tenantId);
  }

  public Future<C> getByQuery(Q query, int offset, int limit, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String where = query.toWhereClause();
    String orderBy = query.toOrderByClause();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getColumns(), getTableName(), where, orderBy, offset, limit);
    log.info("Attempting get by query: {}", sql);
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toCollection);
  }

  public void getByQuery(Q query, int offset, int limit, String tenantId, Handler<E> handler, Handler<AsyncResult<Void>> endHandler) {
    String where = query.toWhereClause();
    String orderBy = query.toOrderByClause();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getColumns(), getTableName(), where, orderBy, offset, limit);
    log.info("Attempting stream get by query: {}", sql);
    postgresClientFactory.createInstance(tenantId).getClient().getConnection(connection -> {
      if (connection.failed()) {
        log.error("Failed to get database connection", connection.cause());
        endHandler.handle(Future.failedFuture(connection.cause()));
        return;
      }
      connection.result().queryStream(sql, stream -> {
        if (stream.failed()) {
          log.error("Failed to get stream", stream.cause());
          endHandler.handle(Future.failedFuture(stream.cause()));
          return;
        }
        stream.result()
          .handler(row -> handler.handle(toEntity(row)))
          .exceptionHandler(e -> endHandler.handle(Future.failedFuture(e)))
          .endHandler(x -> endHandler.handle(Future.succeededFuture()));
      });
    });
  }

  public Future<E> save(E entity, String tenantId) {
    Promise<E> promise = Promise.promise();
    String columns = getColumns();
    String sql = String.format(SAVE_SQL_TEMPLATE, getTableName(), columns, getValuesTemplate(columns));
    log.info("Attempting save: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, toParams(entity, true), save -> {
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
    // NOTE: update when raml-module-builder supports batch save with params
    // vertx-mysql-postgresql-client does not implement batch save
    // vertx-pg-client does
    // https://github.com/folio-org/raml-module-builder/pull/640
    CompositeFuture.all(entities.stream().map(entity -> save(entity, tenantId)).collect(Collectors.toList())).setHandler(batch -> {
      if (batch.failed()) {
        log.error("Failed to batch insert rows in {}", batch.cause(), getTableName());
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
    String columns = getColumns();
    String sql = String.format(UPDATE_SQL_TEMPLATE, getTableName(), columns, getValuesTemplate(columns), id);
    log.info("Attempting update: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, toParams(entity, false), update -> {
      if (update.failed()) {
        log.error("Failed to update row in {} with id {}", update.cause(), getTableName(), id);
        promise.fail(update.cause());
        return;
      }
      if (update.result().getUpdated() == 0) {
        promise.fail(new NotFoundException(String.format("%s row with id %s was not updated", getTableName(), id)));
        return;
      }
      promise.complete(postUpdate(entity));
    });
    return promise.future();
  }

  public Future<Boolean> delete(String id, String tenantId) {
    Promise<UpdateResult> promise = Promise.promise();
    String sql = String.format(DELETE_SQL_TEMPLATE, getTableName(), id);
    log.info("Attempting delete: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, promise);
    return promise.future().map(updateResult -> updateResult.getUpdated() == 1);
  }

  /**
   * Prepare list of params for multi-row INSERT and UPDATE query values
   * 
   * @param entities              list of Entities for extracting values for params
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return list of {@link JsonArray} params
   */
  public List<JsonArray> toParams(List<E> entities, boolean generateIdIfNotExists) {
    return entities.stream().map(entity -> toParams(entity, generateIdIfNotExists)).collect(Collectors.toList());
  }

  /**
   * Convert {@link ResultSet} into Entity
   * 
   * @param resultSet {@link ResultSet} query results
   * @return optional Entity
   */
  public Optional<E> toEntity(ResultSet resultSet)  {
    return resultSet.getNumRows() > 0 ? Optional.of(toEntity(resultSet.getRows().get(0))) : Optional.empty();
  }

  /**
   * Prepare values list for INSERT and UPDATE queries
   * 
   * @param columns comma seperated list of column names
   * @return comma seperated list of question marks matching number of columns
   */
  public String getValuesTemplate(String columns) {
    return Arrays.asList(columns.split(COMMA)).stream()
      .map(c -> QUESTION_MARK)
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
    Promise<ResultSet> promise = Promise.promise();
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
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
   * Prepare params for INSERT and UPDATE query values
   * 
   * @param entity                Entity for extracting values for params
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return {@link JsonArray} params
   */
  protected abstract JsonArray toParams(E entity, boolean generateIdIfNotExists);

  /**
   * Convert {@link ResultSet} into Entity Collection
   * 
   * @param resultSet {@link ResultSet} query results
   * @return Entity Collection
   */
  protected abstract C toCollection(ResultSet resultSet);

  /**
   * Convert {@link JsonObject} into Entity
   * 
   * @param result {@link JsonObject} query result row
   * @return Entity
   */
  protected abstract E toEntity(JsonObject result);

  /**
   * Convert {@link JsonArray} into Entity
   * 
   * @param result {@link JsonArray} query result row
   * @return Entity
   */
  protected abstract E toEntity(JsonArray row);

}