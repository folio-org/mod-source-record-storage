package org.folio.dao;

import java.text.SimpleDateFormat;
import java.util.Optional;

import javax.ws.rs.NotFoundException;

import org.folio.dao.filter.BeanFilter;
import org.folio.rest.persist.PostgresClient;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;

public interface BeanDao<B, C, F extends BeanFilter> {

  public static final String GET_BY_ID_SQL_TEMPLATE = "SELECT * FROM %s WHERE id = '%s';";
  public static final String GET_BY_FILTER_SQL_TEMPLATE = "SELECT * FROM %s %s OFFSET %s LIMIT %s;";
  public static final String SAVE_SQL_TEMPLATE = "INSERT INTO %s (%s) VALUES (%s);";
  public static final String UPDATE_SQL_TEMPLATE = "UPDATE %s SET (%s) = (%s) WHERE id = '%s';";
  public static final String DELETE_SQL_TEMPLATE = "DELETE FROM %s WHERE id = '%s';";

  public static final SimpleDateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

  public default Future<Optional<B>> getById(String id, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(GET_BY_ID_SQL_TEMPLATE, getTableName(), id);
    getLogger().debug("Attempting get by id: {}", sql);
    getPostgresClient(tenantId).select(sql, promise);
    return promise.future().map(this::toBean);
  }

  public default Future<C> getByFilter(F filter, int offset, int limit, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    String sql = String.format(GET_BY_FILTER_SQL_TEMPLATE, getTableName(), filter.toWhereClause(), offset, limit);
    getLogger().debug("Attempting get by filter: {}", sql);
    getPostgresClient(tenantId).select(sql, promise);
    return promise.future().map(this::toCollection);
  }

  public default Future<B> save(B bean, String tenantId) {
    Promise<B> promise = Promise.promise();
    String sql = String.format(SAVE_SQL_TEMPLATE, getTableName(), toColumns(bean), toValues(bean, true));
    getLogger().debug("Attempting save: {}", sql);
    getPostgresClient(tenantId).execute(sql, save -> {
      if (save.failed()) {
        getLogger().error("Failed to insert row in {}", save.cause(), getTableName());
        promise.fail(save.cause());
        return;
      }
      promise.complete(bean);
    });
    return promise.future();
  }

  public default Future<B> update(B bean, String tenantId) {
    Promise<B> promise = Promise.promise();
    String id = getId(bean);
    String sql = String.format(UPDATE_SQL_TEMPLATE, getTableName(), toColumns(bean), toValues(bean, false), id);
    getLogger().debug("Attempting update: {}", sql);
    getPostgresClient(tenantId).execute(sql, update -> {
      if (update.failed()) {
        getLogger().error("Failed to update row in {} with id {}", update.cause(), getTableName(), id);
        promise.fail(update.cause());
        return;
      }
      if (update.result().getUpdated() == 0) {
        promise.fail(new NotFoundException(String.format("%s row with id %s was not updated", getTableName(), id)));
        return;
      }
      promise.complete(bean);
    });
    return promise.future();
  }

  public default Future<Boolean> delete(String id, String tenantId) {
    Promise<UpdateResult> promise = Promise.promise();
    String sql = String.format(DELETE_SQL_TEMPLATE, getTableName(), id);
    getLogger().debug("Attempting delete: {}", sql);
    getPostgresClient(tenantId).execute(sql, promise);
    return promise.future().map(updateResult -> updateResult.getUpdated() == 1);
  }

  public Logger getLogger();

  public PostgresClient getPostgresClient(String tenantId);

  public String getTableName();

  public String getId(B bean);

  public String toColumns(B bean);

  public String toValues(B bean, boolean generateIdIfNotExists);

  public Optional<B> toBean(ResultSet resultSet);

  public C toCollection(ResultSet resultSet);

  public B toBean(JsonObject result);

}