package org.folio.dao;

import com.zaxxer.hikari.pool.HikariPool;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.reactivex.core.Vertx;
import org.folio.TestUtil;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.tools.utils.Envs;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  private Path certFilePath;
  private String originalUserHome;

  private static final String SERVER_PEM = TestUtil.resourceToString("/tls/server-san.crt");
  private static final String SERVER_WRONG_PEM = TestUtil.resourceToString("/tls/server_wrong.crt");

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final String POSTGRES = "postgres";

  @Container
  private static final PostgreSQLContainer<?> postgres =
    new PostgreSQLContainer<>(PostgresTesterContainer.getImageName())
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.key", 0444), "/server.key")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server-san.crt", 0444), "/server.crt")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/init.sh", 0555), "/docker-entrypoint-initdb.d/init.sh")
      .withExposedPorts(5432)
      .withUsername(USERNAME)
      .withPassword(PASSWORD)
      .withDatabaseName(POSTGRES);

  @BeforeEach
  void setUpEach() throws IOException {
    originalUserHome = System.getProperty("user.home");
    System.setProperty("user.home", tempHomeDir.toString());

    certFilePath = Paths.get(tempHomeDir.toString(), ".postgresql", "root.crt");
    Files.createDirectories(certFilePath.getParent());
  }

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

  @Test
  void reactiveSsl(Vertx vertx, VertxTestContext vtc) throws IOException {
    config(SERVER_PEM);
    Files.writeString(certFilePath, SERVER_PEM, StandardCharsets.UTF_8);
    PostgresClientFactory.getCachedPool(vertx, "reactivessl")
      .query(DSL.selectOne().getSQL())
      .execute()
      .onComplete(vtc.succeeding(rowSet -> {
        assertThat(rowSet.rowCount(), is(1));
        vtc.completeNow();
      }));
  }

  @Test
  void reactiveNoSsl(Vertx vertx, VertxTestContext vtc) throws IOException {
    // client without SERVER_PEM must not connect to server
    config(null);
    Files.writeString(certFilePath, SERVER_PEM, StandardCharsets.UTF_8);
    PostgresClientFactory.getCachedPool(vertx, "reactivenossl")
      .query(DSL.selectOne().getSQL())
      .execute()
      .onComplete(vtc.failing(e -> {
        assertThat(e.getMessage(), containsString("no encryption"));
        vtc.completeNow();
      }));
  }

  @Test
  void reactiveWrongCert(Vertx vertx, VertxTestContext vtc) throws IOException {
    // client must reject if SERVER_PEM doesn't match the server key
    config(SERVER_WRONG_PEM);
    Files.writeString(certFilePath, SERVER_WRONG_PEM, StandardCharsets.UTF_8);
    PostgresClientFactory.getCachedPool(vertx, "reactivewrongcert")
      .query(DSL.selectOne().getSQL())
      .execute()
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
  void jdbcSsl() throws SQLException, IOException {
    config(SERVER_PEM);
    Files.writeString(certFilePath, SERVER_PEM, StandardCharsets.UTF_8);
    assertThat(jdbc3("jdbcssl"), is(3));
  }

  @Test
  void jdbcNoSsl() {
    // client without SERVER_PEM must not connect to server
    config(null);
    var e = assertThrows(HikariPool.PoolInitializationException.class, () -> jdbc3("jdbcnossl"));
    assertThat(e.getCause().getMessage(), containsString("no encryption"));
  }

  @Test
  void jdbcWrongCert() throws IOException {
    // client must reject if SERVER_PEM doesn't match the server key
    config(SERVER_WRONG_PEM);
    Files.writeString(certFilePath, SERVER_WRONG_PEM, StandardCharsets.UTF_8);
    var e = assertThrows(HikariPool.PoolInitializationException.class, () -> jdbc3("jdbcwrongssl"));
    assertThat(e.getCause().getCause().getCause().getCause(), is(instanceOf(CertPathValidatorException.class)));
  }

  @AfterEach
  void tearDownEach() {
    System.setProperty("user.home", originalUserHome);
  }

  @AfterAll
  static void afterAll() {
    setConfig(Map.of());
  }

}
