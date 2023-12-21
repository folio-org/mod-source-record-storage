package org.folio.services.util;

import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
public class KafkaUtil {

  /**
   * Extract value of Kafka header from collections of headers
   *
   * @param key     header name
   * @param headers Kafka headers
   * @return value of Kafka header
   */
  public static String extractHeaderValue(String key, List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equalsIgnoreCase(key))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }

  /**
   * Checks whether a specified key-value pair exists within a list of Kafka headers.
   *
   * @param key     The key to search for within the Kafka headers.
   * @param value   The value associated with the key to search for within the Kafka headers.
   * @param headers The list of KafkaHeader objects to search through.
   * @return {@code true} if the key-value pair is found in the headers, {@code false} otherwise.
   */
  public static boolean headerExists(String key, String value, List<KafkaHeader> headers) {
    return headers.stream()
      .anyMatch(kafkaHeader -> key.equals(kafkaHeader.key()) && value.equals(kafkaHeader.value().toString()));
  }
}
