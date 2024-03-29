package org.folio.dao;

import static java.lang.String.format;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import io.netty.handler.ssl.OpenSsl;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.SqlClient;
import org.folio.rest.persist.LoadConfs;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.ModuleName;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

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
  private static final String DB_RECONNECTATTEMPTS = "reconnectAttempts";
  private static final String DB_RECONNECTINTERVAL = "reconnectInterval";
  private static final String MODULE_NAME = ModuleName.getModuleName();
  private static final String VERIFICATION_ALGORITHM = "HTTPS";
  private static final String TRANSPORT_PROTOCOL = "TLSv1.3";
  private static final String SERVER_PEM = "server_pem";

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

  @PostConstruct
  public void setupProxyExecutorClass() {
    // setup proxy class of ReactiveClassicGenericQueryExecutor
    if(numOfRetries != null) QueryExecutorInterceptor.setNumberOfRetries(numOfRetries);
    if(retryDelay != null) QueryExecutorInterceptor.setRetryDelay(retryDelay);
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
   * @throws SQLException
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
    var pgConnectOptions = new PgConnectOptions()
      .setHost(postgresConfig.getString(HOST))
      .setPort(postgresConfig.getInteger(PORT))
      .setDatabase(postgresConfig.getString(DATABASE))
      .setUser(postgresConfig.getString(USERNAME))
      .setPassword(postgresConfig.getString(PASSWORD))
      .setIdleTimeout(postgresConfig.getInteger(IDLE_TIMEOUT, 60000))
      .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
      .setReconnectAttempts(postgresConfig.getInteger(DB_RECONNECTATTEMPTS, 0))
      .setReconnectInterval(postgresConfig.getLong(DB_RECONNECTINTERVAL, 1L))
      .addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
    var serverPem = postgresConfig.getString(SERVER_PEM);
    if (serverPem != null) {
      pgConnectOptions.setSslMode(SslMode.VERIFY_FULL);
      pgConnectOptions.setHostnameVerificationAlgorithm(VERIFICATION_ALGORITHM);
      pgConnectOptions.setPemTrustOptions(new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)));
      pgConnectOptions.setEnabledSecureTransportProtocols(Collections.singleton(TRANSPORT_PROTOCOL));
      if (OpenSSLEngineOptions.isAvailable()) {
        pgConnectOptions.setOpenSslEngineOptions(new OpenSSLEngineOptions());
      } else {
        pgConnectOptions.setJdkSslEngineOptions(new JdkSSLEngineOptions());
        LOG.error("Cannot run OpenSSL, using slow JDKSSL");
      }
      LOG.debug("Enforcing SSL encryption for PostgreSQL connections, requiring {} with server name certificate, using {}",
        TRANSPORT_PROTOCOL, (OpenSSLEngineOptions.isAvailable() ? "OpenSSL " + OpenSsl.versionString() : "JDKSSL"));
    }
    return pgConnectOptions;
  }

  private static DataSource getDataSource(String tenantId) {
    if (DATA_SOURCE_CACHE.containsKey(tenantId)) {
      LOG.debug("getDataSource:: Using existing data source for tenant {}", tenantId);
      return DATA_SOURCE_CACHE.get(tenantId);
    }
    Integer maxPoolSize = postgresConfig.getInteger(DB_MAXPOOLSIZE, DB_MAXPOOLSIZE_DEFAULT_VALUE);
    LOG.info("getDataSource:: Creating new data source for tenant {} with poolSize {}", tenantId, maxPoolSize);
    HikariDataSource dataSource = new HikariDataSource();
    dataSource.setPoolName(format("%s-data-source", tenantId));
    dataSource.setMaximumPoolSize(maxPoolSize);
    dataSource.setMinimumIdle(0);
    dataSource.setJdbcUrl(getJdbcUrl());
    dataSource.setUsername(postgresConfig.getString(USERNAME));
    dataSource.setPassword(postgresConfig.getString(PASSWORD));
    dataSource.setIdleTimeout(postgresConfig.getLong(IDLE_TIMEOUT, 60000L));
    dataSource.setSchema(convertToPsqlStandard(tenantId));
    DATA_SOURCE_CACHE.put(tenantId, dataSource);
    return dataSource;
  }

  private static String getJdbcUrl() {
    return String.format("jdbc:postgresql://%s:%s/%s",
      postgresConfig.getString(HOST), postgresConfig.getInteger(PORT), postgresConfig.getString(DATABASE));
  }

  // using RMB convention driven tenant to schema name
  private static String convertToPsqlStandard(String tenantId){
    return format("%s_%s", tenantId.toLowerCase(), MODULE_NAME);
  }

  private static void close(PgPool client) {
    client.close();
  }

}
