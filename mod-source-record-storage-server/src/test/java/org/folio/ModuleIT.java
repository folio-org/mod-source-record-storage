package org.folio;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.nio.file.Path;

import org.folio.postgres.testing.PostgresTesterContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Integration test executed in "mvn verify" phase.
 *
 * <p>Checks the shaded fat uber jar (this is not tested in "mvn test" unit tests).
 *
 * <p>Checks the Dockerfile (this is not tested in "mvn test" unit tests).
 *
 * <p>How to run this test class only:
 *
 * <p>mvn verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=ModuleIT
 *
 * <p>How to run this test class only without rebuilding the jar file:
 *
 * <p>mvn resources:testResources compiler:testCompile failsafe:integration-test -Dit.test=ModuleIT
 */
@Testcontainers
class ModuleIT {

  private static final Logger LOG = LoggerFactory.getLogger(ModuleIT.class);
  /** Testcontainers logging, requires log4j-slf4j2-impl in test scope */
  private static final boolean IS_LOG_ENABLED = false;
  private static final String SERVER_PEM = TestUtil.resourceToString("/tls/server.crt");
  private static final Network network = Network.newNetwork();

  @Container
  private static final KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"))
      .withNetwork(network)
      .withNetworkAliases("ourkafka");

  @Container
  private static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>(PostgresTesterContainer.getImageName())
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.key", 0444), "/server.key")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/server.crt", 0444), "/server.crt")
      .withCopyFileToContainer(MountableFile.forClasspathResource("tls/init.sh", 0555), "/docker-entrypoint-initdb.d/init.sh")
      .withNetwork(network)
      .withNetworkAliases("postgres")
      .withExposedPorts(5432)
      .withUsername("username")
      .withPassword("password")
      .withDatabaseName("postgres");

  @Container
  private static final GenericContainer<?> mod =
      new GenericContainer<>(
          new ImageFromDockerfile("mod-source-record-storage").withDockerfile(Path.of("../Dockerfile")))
      .dependsOn(kafka, postgres)
      .withNetwork(network)
      .withNetworkAliases("mod-source-record-storage")
      .withExposedPorts(8081)
      .withEnv("DB_HOST", "postgres")
      .withEnv("DB_PORT", "5432")
      .withEnv("DB_USERNAME", "username")
      .withEnv("DB_PASSWORD", "password")
      .withEnv("DB_DATABASE", "postgres")
      .withEnv("DB_SERVER_PEM", SERVER_PEM)
      .withEnv("KAFKA_HOST", "ourkafka")
      .withEnv("KAFKA_PORT", "9092");

  @BeforeAll
  static void beforeAll() {
    RestAssured.reset();
    RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    RestAssured.baseURI = "http://" + mod.getHost() + ":" + mod.getFirstMappedPort();
    if (IS_LOG_ENABLED) {
      kafka.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams().withPrefix("kafka"));
      postgres.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams().withPrefix("pg"));
      mod.followOutput(new Slf4jLogConsumer(LOG).withSeparateOutputStreams().withPrefix("mod"));
    }
  }

  @BeforeEach
  void beforeEach() {
    RestAssured.requestSpecification = null;  // unset X-Okapi-Tenant etc.
  }

  @Test
  @DisplayName("Test health check")
  void health() {
    // request without X-Okapi-Tenant
    when().
      get("/admin/health").
    then().
      statusCode(200).
      body(is("\"OK\""));
  }

  /**
   * Test logging. It broke several times caused by dependency order in pom.xml or by configuration:
   * <a href="https://folio-org.atlassian.net/browse/EDGPATRON-90">https://folio-org.atlassian.net/browse/EDGPATRON-90</a>
   * <a href="https://folio-org.atlassian.net/browse/CIRCSTORE-263">https://folio-org.atlassian.net/browse/CIRCSTORE-263</a>
   * <a href="https://folio-org.atlassian.net/browse/MODINVUP-91">https://folio-org.atlassian.net/browse/MODINVUP-91</a>
   */
  @Test
  @DisplayName("Test can log module")
  void canLog() {
    setTenant("logtenant");

    var path = "/source-storage/records/85ce8fe9-8e89-48f9-bdcc-764b8cf5c968";

    when().
      get(path).
    then().
      statusCode(greaterThanOrEqualTo(400));

    assertThat(mod.getLogs(), containsString("GET " + path));
  }

  @Test
  @DisplayName("Install the module")
  void install() {
    setTenant("install");

    JsonObject body = new JsonObject()
        .put("module_to", "999999.0.0")
        .put("parameters", new JsonArray()
          .add(new JsonObject().put("key", "loadReference").put("value", "true"))
          .add(new JsonObject().put("key", "loadSample").put("value", "true")));

    postTenant(body);
  }

  private void setTenant(String tenant) {
    RestAssured.requestSpecification = new RequestSpecBuilder()
        .addHeader("X-Okapi-Url", "http://mod-source-record-storage:8081")  // returns 404 for all other APIs
        .addHeader("X-Okapi-Tenant", tenant)
        .setContentType(ContentType.JSON)
        .build();
  }

  private void postTenant(JsonObject body) {
    given()
        .body(body.encodePrettily())
        .when()
        .post("/_/tenant")
        .then()
        .statusCode(204);
  }

}
