package org.folio;

import io.vertx.core.Vertx;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.folio.dao.PostgresClientFactory;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public final class TestUtil {

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
