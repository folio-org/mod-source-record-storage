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

  protected final Logger logger = LoggerFactory.getLogger(this.getClass());

  @Autowired
  protected PostgresClientFactory postgresClientFactory;

  public Future<Optional<B>> getById(String id, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(GET_BY_ID_SQL_TEMPLATE, getTableName(), id);
    logger.info("Attempting get by id: {}", sql);
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toBean);
  }

  public Future<C> getByFilter(F filter, int offset, int limit, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getTableName(), filter.toWhereClause(), offset, limit);
    logger.info("Attempting get by filter: {}", sql);
    postgresClientFactory.createInstance(tenantId).select(sql, promise);
    return promise.future().map(this::toCollection);
  }

  public Future<B> save(B bean, String tenantId) {
    Promise<B> promise = Promise.promise();
    String columns = getColumns();
    String sql = String.format(SAVE_SQL_TEMPLATE, getTableName(), columns, getValues(columns));
    logger.info("Attempting save: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, toParams(bean, true), save -> {
      if (save.failed()) {
        logger.error("Failed to insert row in {}", save.cause(), getTableName());
        promise.fail(save.cause());
        return;
      }
      promise.complete(postSave(bean));
    });
    return promise.future();
  }

  public Future<List<B>> save(List<B> beans, String tenantId) {
    Promise<List<B>> promise = Promise.promise();
    logger.info("Attempting batch save in {}", getTableName());
    // NOTE: update when raml-module-builder supports batch save with params
    // vertx-mysql-postgresql-client does not implement batch save
    // vertx-pg-client does
    // https://github.com/folio-org/raml-module-builder/pull/640
    CompositeFuture.all(beans.stream().map(bean -> save(bean, tenantId)).collect(Collectors.toList())).setHandler(batch -> {
      if (batch.failed()) {
        logger.error("Failed to batch insert rows in {}", batch.cause(), getTableName());
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
    String sql = String.format(UPDATE_SQL_TEMPLATE, getTableName(), columns, getValues(columns), id);
    logger.info("Attempting update: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, toParams(bean, false), update -> {
      if (update.failed()) {
        logger.error("Failed to update row in {} with id {}", update.cause(), getTableName(), id);
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
    logger.info("Attempting delete: {}", sql);
    postgresClientFactory.createInstance(tenantId).execute(sql, promise);
    return promise.future().map(updateResult -> updateResult.getUpdated() == 1);
  }

  public List<JsonArray> toParams(List<B> beans, boolean generateIdIfNotExists) {
    return beans.stream().map(bean -> toParams(bean, generateIdIfNotExists)).collect(Collectors.toList());
  }

  public Optional<B> toBean(ResultSet resultSet)  {
    return resultSet.getNumRows() > 0 ? Optional.of(toBean(resultSet.getRows().get(0))) : Optional.empty();
  }

  public String getValues(String columns) {
    return Arrays.asList(columns.split(COMMA)).stream()
      .map(c -> QUESTION_MARK)
      .collect(Collectors.joining(COMMA));
  }

  protected B postSave(B bean) {
    return bean;
  }

  protected B postUpdate(B bean) {
    return bean;
  }

  protected List<B> postSave(List<B> beans) {
    return beans.stream().map(this::postSave).collect(Collectors.toList());
  }

  protected abstract String getColumns();

  protected abstract JsonArray toParams(B bean, boolean generateIdIfNotExists);

  protected abstract C toCollection(ResultSet resultSet);

  protected abstract B toBean(JsonObject result);

}