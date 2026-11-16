package org.folio.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.ResetPeer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.reactivex.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.postgresql.PostgreSQLContainer;

@ExtendWith(VertxExtension.class)
public class PostgresClientFactoryTest {

  private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.9.0";
  private static final int TEST_TIMEOUT_SECONDS = 60;
  private static final int CONCURRENT_POOL_REQUESTS = 16;
  private static final String DEFAULT_CONFIG_FILE_PATH = "/postgres-conf.json";
  private static final String TEST_CONFIG_FILE_PATH = "/postgres-conf-test.json";
  private static final String TENANT_ID = "diku";

  static Vertx vertx;

  /** Every factory a test creates, closed after the test. */
  private final List<PostgresClientFactory> factories = new ArrayList<>();

  @BeforeAll
  static void setUpClass() {
    vertx = Vertx.vertx();
  }

  @AfterAll
  static void tearDownClass() throws Exception {
    CompletableFuture<Void> close = new CompletableFuture<>();
    vertx.close().onComplete(ar -> close.complete(null));
    close.get(30, TimeUnit.SECONDS);
  }

  @DisplayName("should read the database configuration from the environment variables")
  @Test
  void shouldReadConfig_fromEnvironmentVariables() {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");

    // act
    var config = factory().getConfig();

    // assert
    assertThat(config.getString("host")).isEqualTo("host");
    assertThat(config.getInteger("port")).isEqualTo(15432);
    assertThat(config.getString("username")).isEqualTo("username");
    assertThat(config.getString("password")).isEqualTo("password");
    assertThat(config.getString("database")).isEqualTo("database");
  }

  @DisplayName("should read the database configuration from the given configuration file")
  @Test
  void shouldReadConfig_fromGivenConfigFile() {
    // arrange
    Envs.setEnv(new HashMap<>());

    // act
    var postgresClientFactory = factory(TEST_CONFIG_FILE_PATH);

    // assert
    assertThat(postgresClientFactory.getConfigFilePath()).isEqualTo(TEST_CONFIG_FILE_PATH);
    var config = postgresClientFactory.getConfig();
    assertThat(config.getString("host")).isEqualTo("test.host");
    assertThat(config.getInteger("port")).isEqualTo(25432);
    assertThat(config.getString("username")).isEqualTo("test.username");
    assertThat(config.getString("password")).isEqualTo("test.password");
    assertThat(config.getString("database")).isEqualTo("test.database");
  }

  @DisplayName("should fall back to the configuration file path of RMB when none is given")
  @Test
  void shouldUseRmbConfigFilePath_whenNoneIsGiven() {
    // arrange
    Envs.setEnv(new HashMap<>());

    // act
    var postgresClientFactory = factory();

    // assert
    assertThat(postgresClientFactory.getConfigFilePath()).isEqualTo(DEFAULT_CONFIG_FILE_PATH);
  }

  @DisplayName("should build connect options from the database configuration")
  @Test
  void shouldBuildConnectOptions_fromDatabaseConfiguration() {
    // arrange
    Envs.setEnv("db.host", 15432, "db.user", "db.password", "db.database");

    // act
    PgConnectOptions connectOptions = factory().createPgConnectOptions();

    // assert
    assertThat(connectOptions.getHost()).isEqualTo("db.host");
    assertThat(connectOptions.getPort()).isEqualTo(15432);
    assertThat(connectOptions.getUser()).isEqualTo("db.user");
    assertThat(connectOptions.getPassword()).isEqualTo("db.password");
    assertThat(connectOptions.getDatabase()).isEqualTo("db.database");
    assertThat(connectOptions.getProperties())
      .hasEntrySatisfying("application_name",
        name -> assertThat(name).startsWith("mod-source-record-storage-"));
    assertThat(connectOptions.getSslMode()).isEqualTo(SslMode.DISABLE);
  }

  @DisplayName("should require and verify TLS when a server certificate is configured")
  @Test
  void shouldRequireTls_whenServerPemIsConfigured() {
    // arrange
    Envs.setEnv(new HashMap<>(Map.of(
      "DB_HOST", "db.host",
      "DB_PORT", "15432",
      "DB_USERNAME", "db.user",
      "DB_PASSWORD", "db.password",
      "DB_DATABASE", "db.database",
      "DB_SERVER_PEM", "-----BEGIN CERTIFICATE-----"
    )));

    // act
    PgConnectOptions connectOptions = factory().createPgConnectOptions();

    // assert
    assertThat(connectOptions.getSslMode()).isEqualTo(SslMode.VERIFY_FULL);
    assertThat(connectOptions.getSslOptions().getHostnameVerificationAlgorithm()).isEqualTo("HTTPS");
    assertThat(connectOptions.getSslOptions().getEnabledSecureTransportProtocols()).containsExactly("TLSv1.3");
  }

  @DisplayName("should close idle pooled connections after the default release delay "
    + "when no release delay is configured")
  @Test
  void shouldUseDefaultIdleTimeoutAndMaxLifetime_whenNotConfigured() {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");

    // act
    PoolOptions poolOptions = factory().getPoolOptions(15);

    // assert
    assertThat(poolOptions.getIdleTimeout()).isEqualTo(60_000);
    assertThat(poolOptions.getIdleTimeoutUnit()).isEqualTo(TimeUnit.MILLISECONDS);
    assertThat(poolOptions.getMaxLifetime()).isEqualTo(1_800_000);
    assertThat(poolOptions.getMaxLifetimeUnit()).isEqualTo(TimeUnit.MILLISECONDS);
    assertThat(poolOptions.getMaxSize()).isEqualTo(15);
  }

  @DisplayName("should close idle pooled connections after the configured release delay")
  @Test
  void shouldUseConfiguredIdleTimeoutAndMaxLifetime_whenConfigured() {
    // arrange
    Envs.setEnv(new HashMap<>(Map.of(
      "DB_HOST", "host",
      "DB_PORT", "15432",
      "DB_USERNAME", "username",
      "DB_PASSWORD", "password",
      "DB_DATABASE", "database",
      "DB_CONNECTIONRELEASEDELAY", "5000",
      "DB_MAX_LIFETIME", "120000"
    )));

    // act
    PoolOptions poolOptions = factory().getPoolOptions(15);

    // assert
    assertThat(poolOptions.getIdleTimeout()).isEqualTo(5_000);
    assertThat(poolOptions.getIdleTimeoutUnit()).isEqualTo(TimeUnit.MILLISECONDS);
    assertThat(poolOptions.getMaxLifetime()).isEqualTo(120_000);
    assertThat(poolOptions.getMaxLifetimeUnit()).isEqualTo(TimeUnit.MILLISECONDS);
  }

  @DisplayName("should bound unacknowledged data with the default TCP user timeout "
    + "when no timeout is configured")
  @Test
  void shouldUseDefaultTcpUserTimeout_whenNotConfigured() {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");

    // act
    NetClientOptions netClientOptions = factory().getNetClientOptions();

    // assert
    assertThat(netClientOptions.getTcpUserTimeout()).isEqualTo(60_000);
  }

  @DisplayName("should bound unacknowledged data with the configured TCP user timeout")
  @Test
  void shouldUseConfiguredTcpUserTimeout_whenConfigured() {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");
    var postgresClientFactory = factory();
    postgresClientFactory.getConfig().put("tcpUserTimeout", 5_000);

    // act
    NetClientOptions netClientOptions = postgresClientFactory.getNetClientOptions();

    // assert
    assertThat(netClientOptions.getTcpUserTimeout()).isEqualTo(5_000);
  }

  @DisplayName("should evict the cached pool of a tenant when the tenant is closed")
  @Test
  void shouldEvictCachedPool_whenTenantIsClosed(VertxTestContext testContext) {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");
    var postgresClientFactory = factory();
    Pool pool = postgresClientFactory.getCachedPool(TENANT_ID);

    // act
    postgresClientFactory.closeTenant(TENANT_ID)
      // assert
      .onComplete(testContext.succeeding(closed -> testContext.verify(() -> {
        assertThat(postgresClientFactory.getCachedPool(TENANT_ID)).isNotSameAs(pool);
        testContext.completeNow();
      })));
  }

  @DisplayName("should succeed when closing a tenant that has no cached pool")
  @Test
  void shouldSucceed_whenClosingTenantWithoutCachedPool(VertxTestContext testContext) {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");

    // act + assert
    factory().closeTenant("tenant-without-pool")
      .onComplete(testContext.succeedingThenComplete());
  }

  @DisplayName("should create a single pool per tenant when pools are requested concurrently")
  @Test
  void shouldCreateSinglePoolPerTenant_whenRequestedConcurrently() throws InterruptedException {
    // arrange
    Envs.setEnv("host", 15432, "username", "password", "database");
    var postgresClientFactory = factory();
    var executor = Executors.newFixedThreadPool(CONCURRENT_POOL_REQUESTS);
    var requests = Collections.<Callable<Pool>>nCopies(
      CONCURRENT_POOL_REQUESTS, () -> postgresClientFactory.getCachedPool(TENANT_ID));

    // act
    var pools = executor.invokeAll(requests).stream()
      .map(PostgresClientFactoryTest::resultOf)
      .distinct()
      .toList();
    executor.shutdown();

    // assert
    assertThat(pools).hasSize(1);
  }

  @DisplayName("should retry a transaction when the database connection is reset")
  @Test
  void shouldRetryTransaction_whenConnectionIsReset(VertxTestContext testContext)
      throws IOException, InterruptedException {
    Function<PostgresClientFactory, Future<RowSet<Row>>> exec =
      postgresClientFactory -> postgresClientFactory.getQueryExecutor(TENANT_ID)
        .transaction(qe -> qe.execute(dsl -> dsl.select(DSL.inline(1))));
    queryExecutorShouldRetryInternal(testContext, exec);
  }

  private void queryExecutorShouldRetryInternal(VertxTestContext testContext,
      Function<PostgresClientFactory, Future<RowSet<Row>>> exec) throws IOException, InterruptedException {
    // arrange: route the pool through a proxy that resets established connections, so that the
    // executor sees the transport failures (VertxException, ClosedChannelException) it retries on
    try (Network network = Network.newNetwork();
         ToxiproxyContainer toxiproxy = new ToxiproxyContainer(TOXIPROXY_IMAGE);
         PostgreSQLContainer postgreSQLContainer =
           new PostgreSQLContainer(PostgresTesterContainer.DEFAULT_IMAGE_NAME)) {
      toxiproxy.withNetwork(network).withNetworkAliases("toxiproxy").start();
      postgreSQLContainer.withNetwork(network).withNetworkAliases("toxipostgres")
        .waitingFor(new LogMessageWaitStrategy()
          .withRegEx(".*database system is ready to accept connections.*\\n"))
        .start();

      ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
      Proxy proxy = toxiproxyClient.createProxy("postgres", "0.0.0.0:8666", "toxipostgres:5432");
      ResetPeer resetPeer = proxy.toxics().resetPeer("reset-peer", ToxicDirection.DOWNSTREAM, 1000);

      Envs.setEnv(toxiproxy.getHost(), toxiproxy.getMappedPort(8666), "test", "test", "test");
      PostgresClientFactory postgresClientFactory = factory();
      postgresClientFactory.setRetryPolicy(0, 1000L);

      // act + assert
      exec.apply(postgresClientFactory)
        .onComplete(ar1 -> {
          testContext.verify(() -> assertTrue(ar1.failed(), "database execution should fail"));
          postgresClientFactory.setRetryPolicy(5, 1000L);
          exec.apply(postgresClientFactory).onComplete(ar2 -> testContext.verify(() -> {
            assertTrue(ar2.succeeded());
            testContext.completeNow();
          }));
          // make db connections work eventually in 2 seconds
          vertx.setTimer(2000, l -> removeToxic(resetPeer));
        });

      // keep the containers up until the asynchronous assertions are done; the extension reports
      // the outcome of the test context after this method returns
      assertTrue(testContext.awaitCompletion(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "test should complete within " + TEST_TIMEOUT_SECONDS + " seconds");
    }
  }

  @AfterEach
  void cleanup(VertxTestContext testContext) {
    var closeFutures = factories.stream()
      .map(PostgresClientFactory::closeAll)
      .toList();
    factories.clear();
    Envs.setEnv(new HashMap<>());
    Future.all(closeFutures)
      .onComplete(testContext.succeedingThenComplete());
  }

  /**
   * A factory reading its configuration from the environment variables, or from RMB's default
   * configuration file path when none are set, closed after the test.
   *
   * @return the factory
   */
  private PostgresClientFactory factory() {
    var postgresClientFactory = new PostgresClientFactory(vertx);
    factories.add(postgresClientFactory);
    return postgresClientFactory;
  }

  /**
   * A factory reading its configuration from the given configuration file, closed after the test.
   *
   * @param configFilePath path of the configuration file
   * @return the factory
   */
  private PostgresClientFactory factory(String configFilePath) {
    var postgresClientFactory = new PostgresClientFactory(vertx, configFilePath);
    factories.add(postgresClientFactory);
    return postgresClientFactory;
  }

  private static void removeToxic(ResetPeer resetPeer) {
    try {
      resetPeer.remove();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <T> T resultOf(java.util.concurrent.Future<T> future) {
    try {
      return future.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    } catch (ExecutionException | TimeoutException e) {
      throw new IllegalStateException(e);
    }
  }

}
