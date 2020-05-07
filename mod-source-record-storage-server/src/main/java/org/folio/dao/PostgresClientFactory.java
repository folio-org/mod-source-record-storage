package org.folio.dao;

import java.util.HashMap;
import java.util.Map;

import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

@Component
public class PostgresClientFactory {

  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String DATABASE = "database";
  private static final String PASSWORD = "password";
  private static final String USERNAME = "username";

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int POOL_SIZE = 5;

  private static final Map<String, PgPool> pool = new HashMap<>();

  private Vertx vertx;

  public PostgresClientFactory(@Autowired Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Creates instance of Postgres Client
   *
   * @param tenantId tenant id
   * @return Postgres Client
   */
  public PostgresClient createInstance(String tenantId) {
    return PostgresClient.getInstance(vertx, tenantId);
  }

  /**
   * Get database client
   *
   * @return {@link PgPool} database client
   */
  public PgPool getClient(String tenantId) {
    return getPool(tenantId);
  }

  private synchronized PgPool getPool(String tenantId) {
    if (pool.containsKey(tenantId)) {
      return pool.get(tenantId);
    }
    PgConnectOptions connectOptions = getConnectOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(connectOptions, poolOptions);
    pool.put(tenantId, client);
    return client;
  }

  private PgConnectOptions getConnectOptions(String tenantId) {
    JsonObject postgreSQLClientConfig = PostgresClient.getInstance(vertx).getConnectionConfig();
    return new PgConnectOptions()
      .setHost(postgreSQLClientConfig.getString(HOST))
      .setPort(postgreSQLClientConfig.getInteger(PORT))
      .setDatabase(postgreSQLClientConfig.getString(DATABASE))
      .setUser(postgreSQLClientConfig.getString(USERNAME))
      .setPassword(postgreSQLClientConfig.getString(PASSWORD))
      .addProperty(DEFAULT_SCHEMA_PROPERTY, PostgresClient.convertToPsqlStandard(tenantId));
  }

}
