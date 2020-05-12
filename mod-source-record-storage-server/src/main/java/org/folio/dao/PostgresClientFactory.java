package org.folio.dao;

import java.util.HashMap;
import java.util.Map;

import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

@Component
public class PostgresClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresClientFactory.class);

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
   * @param tenantId tenant id
   * @return {@link PgPool} database client
   */
  public PgPool getClient(String tenantId) {
    return getPool(tenantId);
  }

  private synchronized PgPool getPool(String tenantId) {
    if (pool.containsKey(tenantId)) {
      LOG.debug("Using existing database connection pool for tenant {}", tenantId);
      return pool.get(tenantId);
    }
    LOG.info("Creating new database connection pool for tenant {}", tenantId);
    PgConnectOptions connectOptions = getConnectOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(connectOptions, poolOptions);
    pool.put(tenantId, client);
    return client;
  }

  // NOTE: This should be able to get database configuration without PostgresClient.
  // Additionally, with knowledge of tenant at this time, we are not confined to 
  // schema isolation and can provide database isolation.
  private PgConnectOptions getConnectOptions(String tenantId) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx);
    JsonObject postgreSQLClientConfig = postgresClient.getConnectionConfig();
    postgresClient.closeClient(closed -> {
      if (closed.failed()) {
        LOG.error("Unable to close PostgresClient", closed.cause());
      }
    });
    return new PgConnectOptions()
      .setHost(postgreSQLClientConfig.getString(HOST))
      .setPort(postgreSQLClientConfig.getInteger(PORT))
      .setDatabase(postgreSQLClientConfig.getString(DATABASE))
      .setUser(postgreSQLClientConfig.getString(USERNAME))
      .setPassword(postgreSQLClientConfig.getString(PASSWORD))
      // using RMB convention driven tenant to schema name
      .addProperty(DEFAULT_SCHEMA_PROPERTY, PostgresClient.convertToPsqlStandard(tenantId));
  }

}
