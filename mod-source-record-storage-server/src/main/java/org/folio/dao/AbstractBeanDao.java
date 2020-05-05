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

import org.folio.dao.filter.BeanFilter;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;

public abstract class AbstractBeanDao<B, C, F extends BeanFilter> implements BeanDao<B, C, F> {

  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  @Autowired
  protected PostgresClientFactory postgresClientFactory;

  public Future<Optional<B>> getById(String id, String tenantId) {
    String sql = String.format(GET_BY_ID_SQL_TEMPLATE, getTableName(), id);
    log.info("Attempting get by id: {}", sql);
    return select(sql, tenantId);
  }

  public Future<C> getByFilter(F filter, int offset, int limit, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getTableName(), filter.toWhereClause(), offset, limit);
    log.info("Attempting get by filter: {}", sql);
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toCollection);
  }

  public Future<B> save(B bean, String tenantId) {
    Promise<B> promise = Promise.promise();
    String columns = getColumns();
    String sql = String.format(SAVE_SQL_TEMPLATE, getTableName(), columns, getValuesTemplate(columns));
    log.info("Attempting save: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, toParams(bean, true), save -> {
      if (save.failed()) {
        log.error("Failed to insert row in {}", save.cause(), getTableName());
        promise.fail(save.cause());
        return;
      }
      promise.complete(postSave(bean));
    });
    return promise.future();
  }

  public Future<List<B>> save(List<B> beans, String tenantId) {
    Promise<List<B>> promise = Promise.promise();
    log.info("Attempting batch save in {}", getTableName());
    // NOTE: update when raml-module-builder supports batch save with params
    // vertx-mysql-postgresql-client does not implement batch save
    // vertx-pg-client does
    // https://github.com/folio-org/raml-module-builder/pull/640
    CompositeFuture.all(beans.stream().map(bean -> save(bean, tenantId)).collect(Collectors.toList())).setHandler(batch -> {
      if (batch.failed()) {
        log.error("Failed to batch insert rows in {}", batch.cause(), getTableName());
        promise.fail(batch.cause());
        return;
      }
      promise.complete(postSave(beans));
    });
    return promise.future();
  }

  public Future<B> update(B bean, String tenantId) {
    Promise<B> promise = Promise.promise();
    String id = getId(bean);
    String columns = getColumns();
    String sql = String.format(UPDATE_SQL_TEMPLATE, getTableName(), columns, getValuesTemplate(columns), id);
    log.info("Attempting update: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, toParams(bean, false), update -> {
      if (update.failed()) {
        log.error("Failed to update row in {} with id {}", update.cause(), getTableName(), id);
        promise.fail(update.cause());
        return;
      }
      if (update.result().getUpdated() == 0) {
        promise.fail(new NotFoundException(String.format("%s row with id %s was not updated", getTableName(), id)));
        return;
      }
      promise.complete(postUpdate(bean));
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
   * @param beans                 list of Beans for extracting values for params
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return list of {@link JsonArray} params
   */
  public List<JsonArray> toParams(List<B> beans, boolean generateIdIfNotExists) {
    return beans.stream().map(bean -> toParams(bean, generateIdIfNotExists)).collect(Collectors.toList());
  }

  /**
   * Convert {@link ResultSet} into Bean
   * 
   * @param resultSet {@link ResultSet} query results
   * @return optional Bean
   */
  public Optional<B> toBean(ResultSet resultSet)  {
    return resultSet.getNumRows() > 0 ? Optional.of(toBean(resultSet.getRows().get(0))) : Optional.empty();
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
   * @return future of optional Bean
   */
  protected Future<Optional<B>> select(String sql, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toBean);
  }

  /**
   * Post save processing of Bean
   * 
   * @param bean saved Bean
   * @return bean after post save processing
   */
  protected B postSave(B bean) {
    return bean;
  }

  /**
   * Post update processing of Bean
   * 
   * @param bean updated Bean
   * @return bean after post update processing
   */
  protected B postUpdate(B bean) {
    return bean;
  }

  /**
   * Post save processing of list of Beans
   * 
   * @param beans saved list of Beans
   * @return bean after post save processing
   */
  protected List<B> postSave(List<B> beans) {
    return beans.stream().map(this::postSave).collect(Collectors.toList());
  }

  /**
   * Prepare columns list for INSERT and UPDATE queries
   * 
   * @return comma seperated list of table column names
   */
  protected abstract String getColumns();

  /**
   * Prepare params for INSERT and UPDATE query values
   * 
   * @param bean                  Bean for extracting values for params
   * @param generateIdIfNotExists flag indicating whether to generate UUID for id
   * @return {@link JsonArray} params
   */
  protected abstract JsonArray toParams(B bean, boolean generateIdIfNotExists);

  /**
   * Convert {@link ResultSet} into Bean Collection
   * 
   * @param resultSet {@link ResultSet} query results
   * @return Bean Collection
   */
  protected abstract C toCollection(ResultSet resultSet);

  /**
   * Convert {@link JsonObject} into Bean
   * 
   * @param result {@link JsonObject} query result row
   * @return Bean
   */
  protected abstract B toBean(JsonObject result);

}