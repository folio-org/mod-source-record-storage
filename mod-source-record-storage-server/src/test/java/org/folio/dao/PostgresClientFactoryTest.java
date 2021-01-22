package org.folio.dao;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

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

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      async.complete();
    }));
  }

}
