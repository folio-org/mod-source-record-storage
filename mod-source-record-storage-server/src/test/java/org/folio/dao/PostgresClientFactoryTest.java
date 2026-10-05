package org.folio.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.ResetPeer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

@ExtendWith(VertxExtension.class)
public class PostgresClientFactoryTest {

  static Vertx vertx;

  @BeforeAll
  static void setUpClass() {
    vertx = Vertx.vertx();
  }

  @Test
  void shouldCreateFactoryWithDefaultConfigFilePath() {
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    assertEquals("/postgres-conf.json", PostgresClientFactory.getConfigFilePath());
    postgresClientFactory.close();
    PostgresClientFactory.setConfigFilePath(null);
  }

  @Test
  void shouldCreateFactoryWithTestConfig() {
    PostgresClientFactory.setConfigFilePath("/postgres-conf-test.json");
    assertEquals("/postgres-conf-test.json", PostgresClientFactory.getConfigFilePath());
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    JsonObject config = PostgresClientFactory.getConfig();
    assertEquals("test.host", config.getString("host"));
    assertEquals(Integer.valueOf(25432), config.getInteger("port"));
    assertEquals("test.username", config.getString("username"));
    assertEquals("test.password", config.getString("password"));
    assertEquals("test.database", config.getString("database"));
    postgresClientFactory.close();
    Envs.setEnv(new HashMap<>());
    PostgresClientFactory.setConfigFilePath(null);
  }

  @Test
  void shouldCreateFactoryWithConfigFromSpecifiedEnvironment() {
    Envs.setEnv("host", 15432, "username", "password", "database");
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    JsonObject config = PostgresClientFactory.getConfig();
    assertEquals("host", config.getString("host"));
    assertEquals(Integer.valueOf(15432), config.getInteger("port"));
    assertEquals("username", config.getString("username"));
    assertEquals("password", config.getString("password"));
    assertEquals("database", config.getString("database"));
    postgresClientFactory.close();
    Envs.setEnv(new HashMap<>());
    PostgresClientFactory.setConfigFilePath(null);
  }

  @Test
  void shouldSetConfigFilePath() {
    PostgresClientFactory.setConfigFilePath("/postgres-conf-local.json");
    assertEquals("/postgres-conf-local.json", PostgresClientFactory.getConfigFilePath());
    PostgresClientFactory.setConfigFilePath(null);
  }

  @Test
  void queryExecutorTransactionShouldRetry(VertxTestContext testContext) throws IOException {
    Function<PostgresClientFactory, Future<RowSet<Row>>> exec =
      postgresClientFactory -> postgresClientFactory.getQueryExecutor("diku")
        .transaction(qe -> qe.execute(dsl -> dsl.select(DSL.inline(1))));
    queryExecutorShouldRetryInternal(testContext, exec);
  }

  private void queryExecutorShouldRetryInternal(VertxTestContext testContext,
      Function<PostgresClientFactory, Future<RowSet<Row>>> exec) throws IOException {
    Network network = Network.newNetwork();
    ToxiproxyContainer toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.9.0")
      .withNetwork(network).withNetworkAliases("toxiproxy");
    PostgreSQLContainer<?> postgreSQLContainer =
      new PostgreSQLContainer<>(PostgresTesterContainer.DEFAULT_IMAGE_NAME)
        .withNetwork(network).withNetworkAliases("toxipostgres");
    toxiproxy.start();
    postgreSQLContainer
      .waitingFor(new LogMessageWaitStrategy().withRegEx(".*database system is ready to accept connections.*\\n")).start();
    Runnable closeResources = () -> {
      toxiproxy.close();
      postgreSQLContainer.close();
      network.close();
    };
    final ToxiproxyClient toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
    final Proxy proxy = toxiproxyClient.createProxy("postgres", "0.0.0.0:8666", "toxipostgres:5432");
    final String dbHost = toxiproxy.getHost();
    final int dbPort = toxiproxy.getMappedPort(8666);
    ResetPeer resetPeer = proxy.toxics().resetPeer("reset-peer", ToxicDirection.DOWNSTREAM, 1000);

    Envs.setEnv(dbHost, dbPort, "test", "test", "test");
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    postgresClientFactory.setRetryPolicy(0, 1000L);
    exec.apply(postgresClientFactory)
      .onComplete(ar1 -> {
        testContext.verify(() -> assertTrue(ar1.failed(), "database execution should fail"));
        postgresClientFactory.setRetryPolicy(5, 1000L);
        exec.apply(postgresClientFactory).onComplete(ar2 -> testContext.verify(() -> {
          assertTrue(ar2.succeeded());
          closeResources.run();
          testContext.completeNow();
        }));
        // make db connections work eventually in 2 seconds
        vertx.setTimer(2000, l -> {
          try {
            resetPeer.remove();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      });
  }

  @AfterEach
  void cleanup(VertxTestContext testContext) {
    PostgresClientFactory.setConfigFilePath(null);
    Envs.setEnv(new HashMap<>());
    PostgresClientFactory.closeAll()
      .onComplete(testContext.succeedingThenComplete());
  }

  @AfterAll
  static void tearDownClass() throws Exception {
    CompletableFuture<Void> close = new CompletableFuture<>();
    vertx.close().onComplete(ar -> close.complete(null));
    close.get(30, TimeUnit.SECONDS);
  }

}
