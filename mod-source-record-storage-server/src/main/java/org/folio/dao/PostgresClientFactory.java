package org.folio.dao;

import static java.lang.String.format;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ClientSSLOptions;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgBuilder;
import io.vertx.reactivex.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.executor.PgPoolQueryExecutor;
import org.folio.rest.persist.LoadConfs;
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

/**
 * The database clients of the module: a reactive {@link Pool} per tenant for almost every query, and
 * a blocking JDBC {@link DataSource} per tenant for the batch operations that run on worker threads.
 *
 * <p>The module creates a single Spring context ({@code InitAPIImpl}), and the consumer verticles
 * are built from it by {@code SpringVerticleFactory}, so this {@link Component} is one instance per
 * module and holds one pool and one data source per tenant.
 *
 * <p>The configuration and the caches belong to the instance. Constructing a factory therefore has
 * no effect on any other one, and the pools of an instance are reachable only through it, which is
 * what {@link #closeAll()} and {@link #closeTenant(String)} release.
 */
@Component
public class PostgresClientFactory {

  public static final Configuration configuration = new DefaultConfiguration().set(SQLDialect.POSTGRES);
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String DATABASE = "database";
  public static final String PASSWORD = "password";
  public static final String USERNAME = "username";
  public static final String DB_MAXPOOLSIZE = "maxPoolSize";
  public static final String SERVER_PEM = "server_pem";
  private static final Logger LOG = LogManager.getLogger();
  private static final String CONNECTION_TIMEOUT = "DB_CONNECTION_TIMEOUT";
  private static final String DEFAULT_CONNECTION_TIMEOUT_VALUE = "30";
  private static final String TCP_USER_TIMEOUT_ENV = "DB_TCP_USER_TIMEOUT";
  private static final String TCP_USER_TIMEOUT = "tcpUserTimeout";
  private static final int DEFAULT_TCP_USER_TIMEOUT_MS = 60_000;
  private static final String IDLE_TIMEOUT = "connectionReleaseDelay";
  private static final int DEFAULT_IDLE_TIMEOUT_MS = 60_000;
  private static final String MAX_LIFETIME = "maxLifetime";
  private static final int DEFAULT_MAX_LIFETIME_MS = 1_800_000;
  private static final String RECONNECT_ATTEMPTS = "reconnectAttempts";
  private static final String RECONNECT_INTERVAL = "reconnectInterval";
  private static final String APPLICATION_NAME_PROPERTY = "application_name";
  private static final String HOSTNAME_VERIFICATION_ALGORITHM = "HTTPS";
  private static final String TRANSPORT_PROTOCOL = "TLSv1.3";
  private static final String DISABLE_VALUE = "disable";
  private static final String VERIFY_FULL_VALUE = "verify-full";
  private static final int DEFAULT_NUM_OF_RETRIES = 3;
  private static final long DEFAULT_RETRY_DELAY = 1000L;

  private static final String MODULE_NAME = ModuleName.getModuleName();

  private static final String APPLICATION_NAME =
    MODULE_NAME.replace('_', '-') + "-" + ModuleName.getModuleVersion();

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int DB_MAXPOOLSIZE_DEFAULT_VALUE = 15;

  private final Vertx vertx;

  private final String postgresConfigFilePath;

  private final JsonObject postgresConfig;

  /*
   * The caches are reached from every Vert.x context of the module - the REST verticle and the
   * consumer verticles - and the data source cache is also reached from executeBlocking worker
   * threads, so they must be thread safe. Creating an entry has to happen once per tenant: a lost
   * race would open a second pool, doubling the connections of that tenant and leaving a pool that
   * closeAll() never sees.
   */
  private final Map<String, Pool> poolCache = new ConcurrentHashMap<>();

  private final Map<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();

  private String serverPemPath;

  /*
   * Defaults are assigned here as well so that a factory created outside of Spring, as tests do,
   * retries like the module does.
   */
  @Value("${srs.db.reactive.numRetries:" + DEFAULT_NUM_OF_RETRIES + "}")
  private int numOfRetries = DEFAULT_NUM_OF_RETRIES;

  @Value("${srs.db.reactive.retryDelay.ms:" + DEFAULT_RETRY_DELAY + "}")
  private long retryDelay = DEFAULT_RETRY_DELAY;

  @Autowired
  public PostgresClientFactory(io.vertx.core.Vertx vertx) {
    // the config file path is RMB's, it is used only when the DB_* environment variables are unset
    this(vertx, PostgresClient.getConfigFilePath());
  }

  /**
   * Create a factory that reads its database configuration from a specific configuration file.
   *
   * @param vertx          current Vertx
   * @param configFilePath path of the database configuration file, read only when the {@code DB_*}
   *                       environment variables are not set
   */
  public PostgresClientFactory(io.vertx.core.Vertx vertx, String configFilePath) {
    this.vertx = Vertx.newInstance(vertx);
    this.postgresConfigFilePath = configFilePath;
    var environmentConfig = Envs.allDBConfs();
    if (environmentConfig.isEmpty()) {
      LOG.info("DB config not set in environment variables, reading from config file {}", configFilePath);
      this.postgresConfig = LoadConfs.loadConfig(configFilePath);
    } else {
      LOG.info("DB config read from environment variables");
      this.postgresConfig = environmentConfig;
    }
  }

  /**
   * Get {@link PgPoolQueryExecutor} that will attempt to retry an execution
   * on transactions execution in case of certain exceptions occurring.
   *
   * @param tenantId tenant id
   * @return query executor
   */
  public PgPoolQueryExecutor getQueryExecutor(String tenantId) {
    return new PgPoolQueryExecutor(getCachedPool(tenantId).getDelegate(), numOfRetries, retryDelay);
  }

  /**
   * Get {@link Pool}
   *
   * @param tenantId tenant id
   * @return pooled database client
   */
  public Pool getCachedPool(String tenantId) {
    return poolCache.computeIfAbsent(tenantId, this::createPool);
  }

  /**
   * Close all pools and data sources of this factory.
   *
   * @return future completed when every pool is closed
   */
  public Future<Void> closeAll() {
    closeDataSources();
    List<Future<Void>> closeFutures = poolCache.values()
      .stream()
      .map(PostgresClientFactory::closePool)
      .toList();

    // clear on completion (not only on success) so a failed close never leaves
    // a stale pool cached that still points at an already stopped database
    return Future.all(closeFutures)
      .onComplete(ar -> poolCache.clear())
      .mapEmpty();
  }

  /**
   * Close and evict the cached pool and data source of a single tenant, for example when the tenant
   * is purged. The tenant's schema is then dropped, and the connections of that tenant have their
   * {@code search_path} set to it, so they must not stay in the caches. Closing them before the
   * schema is dropped also keeps a pooled session from blocking the {@code DROP SCHEMA}.
   *
   * @param tenantId tenant id
   * @return future completed when the tenant's pool is closed
   */
  public Future<Void> closeTenant(String tenantId) {
    closeDataSource(dataSourceCache.remove(tenantId));
    var pool = poolCache.remove(tenantId);
    if (pool == null) {
      return Future.succeededFuture();
    }
    LOG.info("closeTenant:: Closing database connections of tenant {}", tenantId);
    return closePool(pool);
  }

  @PreDestroy
  public void close() {
    closeAll();
  }

  protected void setRetryPolicy(int retries, long retryDelay) {
    this.numOfRetries = retries;
    this.retryDelay = retryDelay;
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
   * Getter used for testing.
   *
   * @return postgres config
   */
  JsonObject getConfig() {
    return postgresConfig;
  }

  /**
   * Getter used for testing.
   *
   * @return postgres config path
   */
  String getConfigFilePath() {
    return postgresConfigFilePath;
  }

  /**
   * Pool options for the reactive pool, aligned with RMB's
   * {@code PostgresClientInitializer}: an idle pooled connection is closed after
   * {@code DB_CONNECTIONRELEASEDELAY} (60 s by default) and no pooled connection lives longer than
   * {@code DB_MAX_LIFETIME} (30 min by default).
   *
   * <p>Without these timeouts the pool keeps every connection it has ever opened until the module
   * stops. A connection that sits idle for hours can be silently broken by anything on the network
   * path (NAT/conntrack, a firewall, a Pgpool VIP failover), and the next query sent on it then
   * hangs until the kernel gives up retransmitting (~15 min with the Linux default
   * {@code tcp_retries2=15}).
   *
   * @param maxPoolSize maximum number of connections in the pool
   * @return pool options
   */
  PoolOptions getPoolOptions(int maxPoolSize) {
    int connectionTimeout =
      Integer.parseInt(System.getenv().getOrDefault(CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT_VALUE));
    return new PoolOptions()
      .setConnectionTimeout(connectionTimeout)
      .setConnectionTimeoutUnit(TimeUnit.SECONDS)
      .setIdleTimeout(postgresConfig.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT_MS))
      .setIdleTimeoutUnit(TimeUnit.MILLISECONDS)
      .setMaxLifetime(postgresConfig.getInteger(MAX_LIFETIME, DEFAULT_MAX_LIFETIME_MS))
      .setMaxLifetimeUnit(TimeUnit.MILLISECONDS)
      .setMaxSize(maxPoolSize);
  }

  /**
   * Connect options of the reactive pool, built from the module's database configuration.
   *
   * <p>This is the set of options RMB's {@code PostgresClientInitializer} builds for its own pool.
   * RMB keeps that method package private, so the module owns it here instead of reaching into
   * RMB's package: any option the pool needs is then visible and testable in one place, which is
   * what the lost idle timeout of MODSOURCE-742 showed to be necessary. Read replicas
   * ({@code DB_HOST_READER}) are not supported, every connection goes to {@code DB_HOST}.
   *
   * <p>An option is left at its {@link PgConnectOptions} default when the configuration does not
   * set it.
   *
   * @return connect options
   */
  PgConnectOptions createPgConnectOptions() {
    var connectOptions = new PgConnectOptions()
      .addProperty(APPLICATION_NAME_PROPERTY, APPLICATION_NAME);

    var host = postgresConfig.getString(HOST);
    if (host != null) {
      connectOptions.setHost(host);
    }
    var port = postgresConfig.getInteger(PORT);
    if (port != null) {
      connectOptions.setPort(port);
    }
    var username = postgresConfig.getString(USERNAME);
    if (username != null) {
      connectOptions.setUser(username);
    }
    var password = postgresConfig.getString(PASSWORD);
    if (password != null) {
      connectOptions.setPassword(password);
    }
    var database = postgresConfig.getString(DATABASE);
    if (database != null) {
      connectOptions.setDatabase(database);
    }
    var reconnectAttempts = postgresConfig.getInteger(RECONNECT_ATTEMPTS);
    if (reconnectAttempts != null) {
      connectOptions.setReconnectAttempts(reconnectAttempts);
    }
    var reconnectInterval = postgresConfig.getLong(RECONNECT_INTERVAL);
    if (reconnectInterval != null) {
      connectOptions.setReconnectInterval(reconnectInterval);
    }
    var serverPem = postgresConfig.getString(SERVER_PEM);
    if (serverPem != null) {
      setUpSsl(connectOptions, serverPem);
    }
    return connectOptions;
  }

  /**
   * Transport options of the reactive pool. They carry {@code tcpUserTimeout}, which bounds how long
   * transmitted data may stay unacknowledged before the operating system aborts the connection
   * ({@code TCP_USER_TIMEOUT} on Linux). Since Vert.x 5 these settings live on the client builder,
   * not on {@link PgConnectOptions}.
   *
   * @return transport options
   */
  NetClientOptions getNetClientOptions() {
    return new NetClientOptions()
      .setTcpUserTimeout(getTcpUserTimeout());
  }

  /**
   * Close and evict every cached {@link DataSource}. Without this the cache would keep Hikari pools
   * bound to a database (e.g. a test container) that has already been stopped, causing
   * "connection refused" on reuse.
   */
  private void closeDataSources() {
    dataSourceCache.values().forEach(PostgresClientFactory::closeDataSource);
    dataSourceCache.clear();
  }

  private Pool createPool(String tenantId) {
    Integer maxPoolSize = postgresConfig.getInteger(DB_MAXPOOLSIZE, DB_MAXPOOLSIZE_DEFAULT_VALUE);
    LOG.info("createPool:: Creating new database connection for tenant {} with poolSize {}", tenantId, maxPoolSize);
    PgConnectOptions connectOptions = getConnectOptions(tenantId);
    PoolOptions poolOptions = getPoolOptions(maxPoolSize);

    return PgBuilder.pool()
      .with(poolOptions)
      .with(getNetClientOptions())
      .connectingTo(connectOptions)
      .using(vertx)
      .build();
  }

  private PgConnectOptions getConnectOptions(String tenantId) {
    return createPgConnectOptions()
      .addProperty(DEFAULT_SCHEMA_PROPERTY, convertToPsqlStandard(tenantId));
  }

  /**
   * Require TLS and verify the server against the configured certificate, the same way the JDBC data
   * source does with {@code sslrootcert}.
   *
   * @param connectOptions options to configure
   * @param serverPem      certificate in PEM format, one or more concatenated certificates
   */
  private static void setUpSsl(PgConnectOptions connectOptions, String serverPem) {
    connectOptions.setSslMode(SslMode.VERIFY_FULL);
    connectOptions.setSslOptions(new ClientSSLOptions()
      .setHostnameVerificationAlgorithm(HOSTNAME_VERIFICATION_ALGORITHM)
      .setTrustOptions(new PemTrustOptions().addCertValue(Buffer.buffer(serverPem)))
      .setEnabledSecureTransportProtocols(Set.of(TRANSPORT_PROTOCOL)));
  }

  /**
   * How long transmitted data may stay unacknowledged before the operating system aborts the
   * connection, in milliseconds. 0 keeps the operating system default, which is about 15.5 minutes
   * with {@code tcp_retries2=15}: a query sent on a connection that was silently broken by anything
   * on the network path (NAT or conntrack, a firewall, a Pgpool VIP failover) blocks for that long
   * before it fails. It does not limit how long a query may run, because the database acknowledges
   * the query before executing it.
   *
   * @return timeout in milliseconds, 0 to keep the operating system default
   */
  private int getTcpUserTimeout() {
    var fromEnv = System.getenv(TCP_USER_TIMEOUT_ENV);
    if (fromEnv != null) {
      return Integer.parseInt(fromEnv);
    }
    return postgresConfig.getInteger(TCP_USER_TIMEOUT, DEFAULT_TCP_USER_TIMEOUT_MS);
  }

  private DataSource getDataSource(String tenantId) {
    return dataSourceCache.computeIfAbsent(tenantId, this::createDataSource);
  }

  private DataSource createDataSource(String tenantId) {
    Integer maxPoolSize = postgresConfig.getInteger(DB_MAXPOOLSIZE, DB_MAXPOOLSIZE_DEFAULT_VALUE);
    LOG.info("createDataSource:: Creating new data source for tenant {} with poolSize {}", tenantId, maxPoolSize);

    var config = new HikariConfig();
    config.setDataSource(getPgSimpleDataSource());
    config.setPoolName(format("%s-data-source", tenantId));
    config.setMaximumPoolSize(maxPoolSize);
    config.setMinimumIdle(0);
    config.setIdleTimeout(postgresConfig.getLong(IDLE_TIMEOUT, (long) DEFAULT_IDLE_TIMEOUT_MS));
    config.setSchema(convertToPsqlStandard(tenantId));
    config.setUsername(postgresConfig.getString(USERNAME));
    config.setPassword(postgresConfig.getString(PASSWORD));

    return new HikariDataSource(config);
  }

  private DataSource getPgSimpleDataSource() {
    var dataSource = new PGSimpleDataSource();
    dataSource.setServerNames(new String[] {postgresConfig.getString(HOST)});
    dataSource.setPortNumbers(new int[] {postgresConfig.getInteger(PORT)});
    dataSource.setDatabaseName(postgresConfig.getString(DATABASE));

    var certificate = postgresConfig.getString(SERVER_PEM);
    LOG.info("Using certificate: {}", StringUtils.isNotBlank(certificate) ? "yes" : "no");
    if (StringUtils.isNotBlank(certificate)) {
      dataSource.setSslMode(VERIFY_FULL_VALUE);
      dataSource.setSslRootCert(getServerPemPath(certificate));
    } else {
      dataSource.setSslMode(DISABLE_VALUE);
    }
    return dataSource;
  }

  /**
   * Write the {@code DB_SERVER_PEM} certificate to a file so that the JDBC driver can trust it via
   * {@code sslrootcert}. The reactive pool gets the same certificate as a value, see
   * {@link #setUpSsl(PgConnectOptions, String)}, but the JDBC driver only accepts a path. Without
   * this the driver falls back to its default trust store location, {@code ~/.postgresql/root.crt},
   * which no FOLIO deployment provides, so {@code DB_SERVER_PEM} would configure TLS for the
   * reactive pool only.
   *
   * <p>The configuration of a factory never changes, so the file is written once per factory. The
   * data sources are created on worker threads, hence the synchronization.
   *
   * @param certificate certificate in PEM format, one or more concatenated certificates
   * @return path of the file holding the certificate
   */
  private synchronized String getServerPemPath(String certificate) {
    if (serverPemPath != null) {
      return serverPemPath;
    }
    try {
      // createTempFile creates the file with owner-only permissions on POSIX file systems
      var path = Files.createTempFile("db-server-", ".crt");
      path.toFile().deleteOnExit();
      Files.writeString(path, certificate);
      serverPemPath = path.toString();
      return serverPemPath;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to store the DB_SERVER_PEM certificate", e);
    }
  }

  /**
   * Close a {@link DataSource}, doing nothing when it is {@code null} or not closeable.
   *
   * @param dataSource data source to close
   */
  private static void closeDataSource(DataSource dataSource) {
    if (dataSource instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.error("Error closing data source", e);
      }
    }
  }

  // using RMB convention driven tenant to schema name
  private static String convertToPsqlStandard(String tenantId) {
    return format("%s_%s", tenantId.toLowerCase(), MODULE_NAME);
  }

  private static Future<Void> closePool(Pool client) {
    try {
      return client.close()
        .recover(throwable -> {
          LOG.error("Error closing client", throwable);
          return Future.succeededFuture();
        });
    } catch (Exception e) {
      LOG.error("Error closing client", e);
      return Future.succeededFuture();
    }
  }

}
