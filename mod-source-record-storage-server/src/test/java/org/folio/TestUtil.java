package org.folio;

import io.vertx.core.Vertx;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

public final class TestUtil {

  private static final int FREE_PORT_RANGE_START = 10_000;
  private static final int FREE_PORT_RANGE_END = 32_000;
  private static final DockerImageName KAFKA_CONTAINER_NAME = DockerImageName.parse("apache/kafka-native:4.2.0");

  /**
   * The {@link PostgresClientFactory} of the module deployed on {@code vertx}, so that a test uses
   * the same pools as the module under test instead of opening its own.
   *
   * @param vertx vertx the module's verticle is deployed on
   * @return the module's factory bean
   */
  public static PostgresClientFactory getPostgresClientFactory(Vertx vertx) {
    var holder = new PostgresClientFactoryHolder();
    SpringContextUtil.autowireDependenciesFromFirstContext(holder, vertx);
    return holder.postgresClientFactory;
  }

  /**
   * A free local port for the module's HTTP server, chosen outside the OS ephemeral port range.
   *
   * <p>{@link NetworkUtils#nextFreePort()} picks from 49152-65535, which overlaps the Linux ephemeral
   * range (32768-60999). Between the probe and the bind, WireMock's dynamic port, the testcontainers
   * port mappings and the Kafka and Postgres client sockets all take ports from that range, so the
   * probed port could be taken by the time the RestVerticle binds it ("Address already in use").
   *
   * @return a port that was free when probed
   */
  public static int nextFreePort() {
    var random = ThreadLocalRandom.current();
    for (int attempt = 0; attempt < 1000; attempt++) {
      int port = random.nextInt(FREE_PORT_RANGE_START, FREE_PORT_RANGE_END);
      if (NetworkUtils.isLocalPortFree(port)) {
        return port;
      }
    }
    throw new IllegalStateException("No free port found in range " + FREE_PORT_RANGE_START + "-" + FREE_PORT_RANGE_END);
  }

  @SuppressWarnings("resource")
  public static KafkaContainer getKafkaContainer() {
    return new KafkaContainer(KAFKA_CONTAINER_NAME)
        .withStartupAttempts(3);
  }

  public static String readFileFromPath(String path) {
    try {
      return FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static String resourceToString(String resource) {
    try {
      return IOUtils.resourceToString(resource, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Target for Spring field injection, used by {@link #getPostgresClientFactory(Vertx)}. */
  private static class PostgresClientFactoryHolder {
    @Autowired
    private PostgresClientFactory postgresClientFactory;
  }

}
