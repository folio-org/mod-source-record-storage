package org.folio.dao;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.folio.rest.persist.PostgresClient;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
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

  public static final Configuration configuration = new DefaultConfiguration().set(SQLDialect.POSTGRES);

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

  @PreDestroy
  public void closeAll() {
    pool.values().forEach(p -> p.close());
    pool.clear();
  }

  /**
   * Creates instance of {@link PostgresClient}
   *
   * @param tenantId tenant id
   * @return postgres client
   */
  public PostgresClient createInstance(String tenantId) {
    return PostgresClient.getInstance(vertx, tenantId);
  }

  /**
   * Get {@link ReactiveClassicGenericQueryExecutor}
   * 
   * @param tenantId tenant id
   * @return reactive query executor
   */
  public ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return new ReactiveClassicGenericQueryExecutor(configuration, getClient(tenantId));
  }

  /**
   * Get {@link PgPool}
   *
   * @param tenantId tenant id
   * @return database client
   */
  public PgPool getClient(String tenantId) {
    return getPool(tenantId);
  }

  private PgPool getPool(String tenantId) {
    // assumes a single thread Vert.x model so no synchronized needed
    if (pool.containsKey(tenantId)) {
      LOG.debug("Using existing database connection pool for tenant {}", tenantId);
      return pool.get(tenantId);
    }
    LOG.info("Creating new database connection pool for tenant {}", tenantId);
    PgConnectOptions connectOptions = getConnectOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
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
