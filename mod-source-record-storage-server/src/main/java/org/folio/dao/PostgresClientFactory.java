package org.folio.dao;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.LoadConfs;
import org.folio.rest.persist.PgConnectOptionsHelper;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.ModuleName;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

@Component
public class PostgresClientFactory {

  private static final Logger LOG = LogManager.getLogger();

  public static final Configuration configuration = new DefaultConfiguration().set(SQLDialect.POSTGRES);

  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String DATABASE = "database";
  public static final String PASSWORD = "password";
  public static final String USERNAME = "username";
  public static final String DB_MAXPOOLSIZE = "maxPoolSize";
  private static final String CONNECTION_TIMEOUT = "DB_CONNECTION_TIMEOUT";
  private static final String DEFAULT_CONNECTION_TIMEOUT_VALUE = "30";
  private static final String IDLE_TIMEOUT = "connectionReleaseDelay";
  public static final String SERVER_PEM = "server_pem";
  private static final String DISABLE_VALUE = "disable";

  private static final String MODULE_NAME = ModuleName.getModuleName();

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int DB_MAXPOOLSIZE_DEFAULT_VALUE = 15;

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();

  private static final Map<String, DataSource> DATA_SOURCE_CACHE = new HashMap<>();

  private static JsonObject postgresConfig;

  private static String postgresConfigFilePath;

  private final Vertx vertx;

  private static Class<? extends ReactiveClassicGenericQueryExecutor> reactiveClassicGenericQueryExecutorProxyClass;

  @Value("${srs.db.reactive.numRetries:3}")
  private Integer numOfRetries;

  @Value("${srs.db.reactive.retryDelay.ms:1000}")
  private Long retryDelay;

  @Autowired
  public PostgresClientFactory(io.vertx.core.Vertx vertx) {
    this.vertx = Vertx.newInstance(vertx);
    // check environment variables for postgres config
    if (!Envs.allDBConfs().isEmpty()) {
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

  @PostConstruct
  public void setupProxyExecutorClass() {
    // setup proxy class of ReactiveClassicGenericQueryExecutor
    if (numOfRetries != null) QueryExecutorInterceptor.setNumberOfRetries(numOfRetries);
    if (retryDelay != null) QueryExecutorInterceptor.setRetryDelay(retryDelay);
    reactiveClassicGenericQueryExecutorProxyClass = QueryExecutorInterceptor.generateClass();
  }

  protected void setRetryPolicy(Integer retries, Long retryDelay) {
    this.numOfRetries = retries;
    this.retryDelay = retryDelay;
    setupProxyExecutorClass();
  }

  @PreDestroy
  public void close() {
    closeAll();
  }

  /**
   * Get proxied {@link ReactiveClassicGenericQueryExecutor} that will attempt to retry an execution
   * on some executions
   *
   * @param tenantId tenant id
   * @return reactive query executor
   */
  public ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    if (reactiveClassicGenericQueryExecutorProxyClass == null) setupProxyExecutorClass();
    ReactiveClassicGenericQueryExecutor queryExecutorProxy;
    try {
      queryExecutorProxy = reactiveClassicGenericQueryExecutorProxyClass
        .getDeclaredConstructor(Configuration.class, SqlClient.class)
        .newInstance(configuration, getCachedPool(this.vertx, tenantId).getDelegate());
    } catch (Exception e) {
      throw new RuntimeException("Something happened while creating proxied reactiveClassicGenericQueryExecutor", e);
    }
    return queryExecutorProxy;
  }

  /**
   * Get {@link PgPool}
   *
   * @param tenantId tenant id
   * @return pooled database client
   */
  public PgPool getCachedPool(String tenantId) {
    return getCachedPool(this.vertx, tenantId);
  }

  /**
   * Get database {@link Connection}
   *
   * @param tenantId tenant id
   * @return pooled database connection
   * @throws SQLException if connection cannot be established
   */
  Connection getConnection(String tenantId) throws SQLException {
    return getDataSource(tenantId).getConnection();
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
      LOG.debug("getCachedPool:: Using existing database connection pool for tenant {}", tenantId);
      return POOL_CACHE.get(tenantId);
    }

    Integer maxPoolSize = postgresConfig.getInteger(DB_MAXPOOLSIZE, DB_MAXPOOLSIZE_DEFAULT_VALUE);
    int connectionTimeout = Integer.parseInt(System.getenv().getOrDefault(CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_VALUE));
    LOG.info("getCachedPool:: Creating new database connection for tenant {} with poolSize {}", tenantId, maxPoolSize);
    PgConnectOptions connectOptions = getConnectOptions(tenantId);
    PoolOptions poolOptions = new PoolOptions()
      .setConnectionTimeout(connectionTimeout)
      .setConnectionTimeoutUnit(TimeUnit.SECONDS)
      .setMaxSize(maxPoolSize);
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, client);
    return client;
  }

  private static PgConnectOptions getConnectOptions(String tenantId) {
    return PgConnectOptionsHelper.createPgConnectOptions(postgresConfig)
      .addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
  }

  private static DataSource getDataSource(String tenantId) {
    if (DATA_SOURCE_CACHE.containsKey(tenantId)) {
      LOG.debug("getDataSource:: Using existing data source for tenant {}", tenantId);
      return DATA_SOURCE_CACHE.get(tenantId);
    }
    Integer maxPoolSize = postgresConfig.getInteger(DB_MAXPOOLSIZE, DB_MAXPOOLSIZE_DEFAULT_VALUE);
    LOG.info("getDataSource:: Creating new data source for tenant {} with poolSize {}", tenantId, maxPoolSize);

    var config = new HikariConfig();
    config.setDataSource(getPgSimpleDataSource());
    config.setPoolName(format("%s-data-source", tenantId));
    config.setMaximumPoolSize(maxPoolSize);
    config.setMinimumIdle(0);
    config.setIdleTimeout(postgresConfig.getLong(IDLE_TIMEOUT, 60000L));
    config.setSchema(convertToPsqlStandard(tenantId));
    config.setUsername(postgresConfig.getString(USERNAME));
    config.setPassword(postgresConfig.getString(PASSWORD));
    var dataSource = new HikariDataSource(config);
    DATA_SOURCE_CACHE.put(tenantId, dataSource);
    return dataSource;
  }

  private static DataSource getPgSimpleDataSource() {
    var dataSource = new PGSimpleDataSource();
    dataSource.setServerNames(new String[]{postgresConfig.getString(HOST)});
    dataSource.setPortNumber(postgresConfig.getInteger(PORT));
    dataSource.setDatabaseName(postgresConfig.getString(DATABASE));

    var certificate = postgresConfig.getString(SERVER_PEM);
    if (StringUtils.isNotBlank(certificate)) {
      dataSource.setSsl(true);
    } else {
      dataSource.setSslMode(DISABLE_VALUE);
    }
    return dataSource;
  }

  // using RMB convention driven tenant to schema name
  private static String convertToPsqlStandard(String tenantId) {
    return format("%s_%s", tenantId.toLowerCase(), MODULE_NAME);
  }

  private static void close(PgPool client) {
    client.close();
  }

}
