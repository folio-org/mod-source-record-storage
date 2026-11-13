package org.folio.dao;

import com.zaxxer.hikari.pool.HikariPool;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.folio.TestUtil;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;
import java.security.cert.CertPathValidatorException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
@ExtendWith(VertxExtension.class)
class PostgresClientFactorySslTest {

  @TempDir
  Path tempHomeDir;

  private String originalUserHome;

  private PostgresClientFactory postgresClientFactory;

  private static final String SERVER_PEM = TestUtil.resourceToString("/tls/server-san.crt");
  private static final String SERVER_WRONG_PEM = TestUtil.resourceToString("/tls/server_wrong.crt");

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String POSTGRES = "postgres";

  @Container
  private static final PostgreSQLContainer postgres =
    new PostgreSQLContainer(PostgresTesterContainer.getImageName())
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.key", 292), "/server.key")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server-san.crt", 292), "/server.crt")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/init.sh", 365), "/docker-entrypoint-initdb.d/init.sh")
      .withExposedPorts(5432)
      .withUsername(USERNAME)
      .withPassword(PASSWORD)
      .withDatabaseName(POSTGRES);

  @BeforeEach
  void setUpEach() {
    // point HOME at an empty directory: both clients must trust DB_SERVER_PEM only, and must not
    // depend on the JDBC driver's default trust store location, ~/.postgresql/root.crt
    originalUserHome = System.getProperty("user.home");
    System.setProperty("user.home", tempHomeDir.toString());
  }

  void config(Vertx vertx, String pem) {
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
    Envs.setEnv(map);
    postgresClientFactory = new PostgresClientFactory(vertx.getDelegate());
  }

  @DisplayName("should connect the reactive pool when DB_SERVER_PEM matches the server certificate")
  @Test
  void shouldConnectReactivePool_whenServerPemMatchesServerCertificate(Vertx vertx, VertxTestContext vtc) {
    // given
    config(vertx, SERVER_PEM);

    // when
    postgresClientFactory.getQueryExecutor("reactivessl").execute(DSLContext::selectOne)
      // then
      .onComplete(vtc.succeeding(rowSet -> {
        assertThat(rowSet.size(), is(1));
        vtc.completeNow();
      }));
  }

  @DisplayName("should fail the reactive pool connection when DB_SERVER_PEM is not configured")
  @Test
  void shouldFailReactiveConnection_whenServerPemIsNotConfigured(Vertx vertx, VertxTestContext vtc) {
    // given a client without SERVER_PEM, which must not connect to the server
    config(vertx, null);

    // when
    postgresClientFactory.getQueryExecutor("reactivenossl").execute(DSLContext::selectOne)
      // then
      .onComplete(vtc.failing(e -> {
        assertThat(e.getMessage(), containsString("no encryption"));
        vtc.completeNow();
      }));
  }

  @DisplayName("should fail the reactive pool connection when DB_SERVER_PEM does not match the server certificate")
  @Test
  void shouldFailReactiveConnection_whenServerPemDoesNotMatchServerCertificate(Vertx vertx, VertxTestContext vtc) {
    // given a certificate that doesn't match the server key, which the client must reject
    config(vertx, SERVER_WRONG_PEM);

    // when
    postgresClientFactory.getQueryExecutor("reactivewrongcert").execute(DSLContext::selectOne)
      // then
      .onComplete(vtc.failing(e -> {
        assertThat(e.getMessage(), containsString("SSL handshake failed"));
        vtc.completeNow();
      }));
  }

  int jdbc3(String tenant) throws SQLException {
    try (Statement statement = postgresClientFactory.getConnection(tenant).createStatement();
         ResultSet resultSet = statement.executeQuery("SELECT 3")) {
      resultSet.next();
      return resultSet.getInt(1);
    }
  }

  @DisplayName("should connect the JDBC data source when DB_SERVER_PEM matches the server certificate")
  @Test
  void shouldConnectJdbcDataSource_whenServerPemMatchesServerCertificate(Vertx vertx) throws SQLException {
    // given
    config(vertx, SERVER_PEM);

    // when + then
    assertThat(jdbc3("jdbcssl"), is(3));
  }

  @DisplayName("should fail the JDBC connection when DB_SERVER_PEM is not configured")
  @Test
  void shouldFailJdbcConnection_whenServerPemIsNotConfigured(Vertx vertx) {
    // given a client without SERVER_PEM, which must not connect to the server
    config(vertx, null);

    // when
    var e = assertThrows(HikariPool.PoolInitializationException.class, () -> jdbc3("jdbcnossl"));

    // then
    assertThat(e.getCause().getMessage(), containsString("no encryption"));
  }

  @DisplayName("should fail the JDBC connection when DB_SERVER_PEM does not match the server certificate")
  @Test
  void shouldFailJdbcConnection_whenServerPemDoesNotMatchServerCertificate(Vertx vertx) {
    // given a certificate that doesn't match the server key, which the client must reject
    config(vertx, SERVER_WRONG_PEM);

    // when
    var e = assertThrows(HikariPool.PoolInitializationException.class, () -> jdbc3("jdbcwrongssl"));

    // then
    assertThat(e.getCause().getCause().getCause().getCause(), is(instanceOf(CertPathValidatorException.class)));
  }

  @AfterEach
  void tearDownEach() {
    System.setProperty("user.home", originalUserHome);
    postgresClientFactory.closeAll();
    Envs.setEnv(Map.of());
  }

}
