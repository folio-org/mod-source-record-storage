package org.folio.dao;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.security.cert.CertPathValidatorException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.folio.TestUtil;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.util.PSQLException;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.testcontainers.utility.MountableFile;

@Testcontainers
@ExtendWith(VertxExtension.class)
class PostgresClientFactorySslTest {
  private static final String SERVER_PEM = TestUtil.resourceToString("/tls/server.crt");
  private static final String SERVER_WRONG_PEM = TestUtil.resourceToString("/tls/server_wrong.crt");

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String POSTGRES = "postgres";

  @Container
  private static final PostgreSQLContainer<?> postgres =
    new PostgreSQLContainer<>(PostgresTesterContainer.getImageName())
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.key", 0444), "/server.key")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.crt", 0444), "/server.crt")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/init.sh", 0555), "/docker-entrypoint-initdb.d/init.sh")
      .withExposedPorts(5432)
      .withUsername(USERNAME)
      .withPassword(PASSWORD)
      .withDatabaseName(POSTGRES);

  static void setConfig(Map<String, String> map) {
    Envs.setEnv(map);
    new PostgresClientFactory(null);
  }

  void config(String pem) {
    var map = Map.of(
      "DB_HOST", postgres.getHost(),
      "DB_PORT", "" + postgres.getFirstMappedPort(),
      "DB_USERNAME", USERNAME,
      "DB_PASSWORD", PASSWORD,
      "DB_DATABASE", POSTGRES);

    if (pem != null) {
      map = new HashMap<>(map);
      map.put("DB_SERVER_PEM", pem);
      map = Map.copyOf(map);
    }
    setConfig(map);
  }

  @AfterAll
  static void afterAll() {
    setConfig(Map.of());
  }

  @Test
  void reactiveSsl(Vertx vertx, VertxTestContext vtc) {
    config(SERVER_PEM);
    PostgresClientFactory.getQueryExecutor(vertx, "reactivessl").execute(dsl -> dsl.selectOne())
      .onComplete(vtc.succeeding(count -> {
        assertThat(count, is(1));
        vtc.completeNow();
      }));
  }

  @Test
  void reactiveNoSsl(Vertx vertx, VertxTestContext vtc) {
    // client without SERVER_PEM must not connect to server
    config(null);
    PostgresClientFactory.getQueryExecutor(vertx, "reactivenossl").execute(dsl -> dsl.selectOne())
      .onComplete(vtc.failing(e -> {
        assertThat(e.getMessage(), containsString("SSL off"));
        vtc.completeNow();
      }));
  }

  @Test
  void reactiveWrongCert(Vertx vertx, VertxTestContext vtc) {
    // client must reject if SERVER_PEM doesn't match the server key
    config(SERVER_WRONG_PEM);
    PostgresClientFactory.getQueryExecutor(vertx, "reactivewrongcert").execute(dsl -> dsl.selectOne())
      .onComplete(vtc.failing(e -> {
        assertThat(e.getMessage(), containsString("SSL handshake failed"));
        vtc.completeNow();
      }));
  }

  int jdbc3(String tenant) throws SQLException {
    try (Statement statement = new PostgresClientFactory(null).getConnection(tenant).createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT 3")) {
      resultSet.next();
      return resultSet.getInt(1);
    }
  }

  @Test
  void jdbcSsl() throws SQLException {
    config(SERVER_PEM);
    assertThat(jdbc3("jdbcssl"), is(3));
  }

  @Test
  void jdbcNoSsl() {
    // client without SERVER_PEM must not connect to server
    config(null);
    var e = assertThrows(PSQLException.class, () -> jdbc3("jdbcnossl"));
    assertThat(e.getMessage(), containsString("SSL off"));
  }

  @Test
  void jdbcWrongCert() {
    // client must reject if SERVER_PEM doesn't match the server key
    config(SERVER_WRONG_PEM);
    var e = assertThrows(PSQLException.class, () -> jdbc3("jdbcwrongssl"));
    assertThat(e.getCause().getCause().getCause(), is(instanceOf(CertPathValidatorException.class)));
  }

}
