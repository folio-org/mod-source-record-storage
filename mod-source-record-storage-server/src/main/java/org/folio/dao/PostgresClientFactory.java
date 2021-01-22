package org.folio.dao;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.folio.rest.persist.LoadConfs;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.Envs;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

@Component
public class PostgresClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresClientFactory.class);

  public static final Configuration configuration = new DefaultConfiguration().set(SQLDialect.POSTGRES);

  private static final String MODULE_NAME = PomReader.INSTANCE.getModuleName();

  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String DATABASE = "database";
  private static final String PASSWORD = "password";
  private static final String USERNAME = "username";
  private static final String IDLE_TIMEOUT = "connectionReleaseDelay";

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int POOL_SIZE = 5;

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();

  private static JsonObject postgresConfig;

  private static String postgresConfigFilePath;

  private final Vertx vertx;

  @Autowired
  public PostgresClientFactory(io.vertx.core.Vertx vertx) {
    this.vertx = Vertx.newInstance(vertx);
    // check environment variables for postgres config
    if (Envs.allDBConfs().size() > 0) {
      LOG.info("DB config read from environment variables");
      postgresConfig = Envs.allDBConfs();
    } else {
      if (Objects.isNull(postgresConfigFilePath)) {
        // need to retrieve config file path from RMB PostgresClient
        postgresConfigFilePath = PostgresClient.getConfigFilePath();
      }
      // no env variables passed in, read for module's config file
      postgresConfig = LoadConfs.loadConfig(postgresConfigFilePath);
    }
  }

  @PreDestroy
  public void close() {
    closeAll();
  }

  /**
   * Get {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param tenantId tenant id
   * @return reactive query executor
   */
  public ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return new ReactiveClassicGenericQueryExecutor(configuration, getCachedPool(this.vertx, tenantId).getDelegate());
  }

  /**
   * Get {@link PgPool}
   *
   * @param tenantId tenant id
   * @return
   */
  public PgPool getCachedPool(String tenantId) {
    return getCachedPool(this.vertx, tenantId);
  }

  /**
   * If used, should be called before any instance of PostgresClientFactory is created.
   * 
   * @param configPath path to postgres config file
   */
  public static void setConfigFilePath(String configPath) {
    postgresConfigFilePath = configPath;
  }

  /**
   * Get {@link ReactiveClassicGenericQueryExecutor} for unit testing.
   *
   * @param vertx    current Vertx
   * @param tenantId tenant id
   * @return reactive query executor
   */
  public static ReactiveClassicGenericQueryExecutor getQueryExecutor(Vertx vertx, String tenantId) {
    return new ReactiveClassicGenericQueryExecutor(configuration, getCachedPool(vertx, tenantId).getDelegate());
  }

  /**
   * Close all cached connections.
   */
  public static void closeAll() {
    POOL_CACHE.values().forEach(PostgresClientFactory::close);
    POOL_CACHE.clear();
  }

  /**
   * Getter used for testing.
   * 
   * @return postgres config
   */
  static JsonObject getConfig() {
    return postgresConfig;
  }

  /**
   * Getter used for testing.
   * 
   * @return postgres config path
   */
  static String getConfigFilePath() {
    return postgresConfigFilePath;
  }

  private static PgPool getCachedPool(Vertx vertx, String tenantId) {
    // assumes a single thread Vert.x model so no synchronized needed
    if (POOL_CACHE.containsKey(tenantId)) {
      LOG.debug("Using existing database connection pool for tenant {}", tenantId);
      return POOL_CACHE.get(tenantId);
    }
    LOG.info("Creating new database connection pool for tenant {}", tenantId);
    PgConnectOptions connectOptions = getConnectOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, client);
    return client;
  }

  private static PgConnectOptions getConnectOptions(String tenantId) {
    return new PgConnectOptions()
      .setHost(postgresConfig.getString(HOST))
      .setPort(postgresConfig.getInteger(PORT))
      .setDatabase(postgresConfig.getString(DATABASE))
      .setUser(postgresConfig.getString(USERNAME))
      .setPassword(postgresConfig.getString(PASSWORD))
      .setIdleTimeout(postgresConfig.getInteger(IDLE_TIMEOUT, 60000))
      .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
      .addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
  }

  // using RMB convention driven tenant to schema name
  private static String convertToPsqlStandard(String tenantId){
    return String.format("%s_%s", tenantId.toLowerCase(), MODULE_NAME);
  }

  private static void close(PgPool client) {
    client.close();
  }

}
