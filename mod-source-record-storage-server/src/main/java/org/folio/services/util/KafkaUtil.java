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
      .filter(header -> header.key().equals(key))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
