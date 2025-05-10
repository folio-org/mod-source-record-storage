package org.folio;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public final class TestUtil {

  private static final DockerImageName KAFKA_CONTAINER_NAME = DockerImageName.parse("apache/kafka-native:3.8.0");

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
}
