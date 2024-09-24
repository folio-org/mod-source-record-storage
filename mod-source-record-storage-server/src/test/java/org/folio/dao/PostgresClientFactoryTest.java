package org.folio.dao;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.ResetPeer;
import io.github.jklingsporn.vertx.jooq.shared.internal.QueryResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.jooq.impl.DSL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class PostgresClientFactoryTest {

  static Vertx vertx;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    vertx = Vertx.vertx();
  }

  @Test
  public void shouldCreateFactoryWithDefaultConfigFilePath(TestContext context) {
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    assertEquals("/postgres-conf.json", PostgresClientFactory.getConfigFilePath());
    postgresClientFactory.close();
    PostgresClientFactory.setConfigFilePath(null);
  }

  @Test
  public void shouldCreateFactoryWithTestConfig(TestContext context) {
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
  public void shouldCreateFactoryWithConfigFromSpecifiedEnvironment(TestContext context) {
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
  public void shouldSetConfigFilePath() {
    PostgresClientFactory.setConfigFilePath("/postgres-conf-local.json");
    assertEquals("/postgres-conf-local.json", PostgresClientFactory.getConfigFilePath());
    PostgresClientFactory.setConfigFilePath(null);
  }

  @Test
  public void queryExecutorTransactionShouldRetry(TestContext context) throws IOException, InterruptedException {
    Function<PostgresClientFactory, Future<QueryResult>> exec =
      (postgresClientFactory) -> postgresClientFactory.getQueryExecutor("diku")
              .transaction(qe -> qe.query(dsl -> dsl.select(DSL.inline(1))));
    queryExecutorShouldRetryInternal(context, exec);
  }

  @Test
  public void queryExecutorQueryShouldRetry(TestContext context) throws IOException, InterruptedException {
    Function<PostgresClientFactory, Future<QueryResult>> exec =
      (postgresClientFactory) -> postgresClientFactory.getQueryExecutor("diku")
        .query(dsl -> dsl.select(DSL.inline(1)));
    queryExecutorShouldRetryInternal(context, exec);
  }

  private void queryExecutorShouldRetryInternal(TestContext context, Function<PostgresClientFactory, Future<QueryResult>> exec)
    throws IOException, InterruptedException {
    Async async = context.async();
    // Arrange
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
    // break connection after 1 second
    ResetPeer resetPeer = proxy.toxics().resetPeer("reset-peer", ToxicDirection.DOWNSTREAM, 1000);

    // Act
    Envs.setEnv(dbHost, dbPort, "test", "test", "test");
    PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
    postgresClientFactory.setRetryPolicy(0, null);
    exec.apply(postgresClientFactory)
      .onComplete(ar1 -> {
        // expect failure
        if (!ar1.failed()) {
          closeResources.run();
          context.fail("database execution should fail");
        }

        // set multiple retries.
        postgresClientFactory.setRetryPolicy(5, null);
        exec.apply(postgresClientFactory).onComplete(ar2 -> {
          // expect success
          context.assertTrue(ar2.succeeded());
          closeResources.run();
          async.complete();
        });
        // make db connections work eventually in 2 seconds
        // if executor is set to retry 5 times, then it will take at least 5 seconds for return of an error
        vertx.setTimer(2000, (l) -> {
          try {
            resetPeer.remove();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      });
  }

  @After
  public void cleanup() {
    PostgresClientFactory.setConfigFilePath(null);
    PostgresClientFactory.closeAll();
    Envs.setEnv(new HashMap<>());
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> async.complete()));
  }

}
