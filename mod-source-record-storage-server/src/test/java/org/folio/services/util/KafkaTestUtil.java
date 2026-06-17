package org.folio.services.util;

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utility class for Kafka-related test operations.
 * Provides helper methods for cleaning up Kafka topics in test environments.
 * This is particularly useful when using testcontainers for integration testing,
 * ensuring that test topics are properly cleared between test runs to prevent
 * data leakage and test interdependencies.
 */
@UtilityClass
public class KafkaTestUtil {

  /**
   * Clears Kafka consumer offsets by seeking to the end of all topic partitions.
   * This method provides an alternative approach to topic cleanup by resetting
   * consumer group offsets to the end of all available partitions. This is useful
   * when you want to preserve the topic data but ensure the consumer group starts
   * reading from the latest offset.
   */
  public static void clearAllTopics(Properties consumerProperties) {
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
      Set<TopicPartition> partitions = consumer.listTopics().values().stream()
        .flatMap(partitionInfos -> partitionInfos.stream()
          .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())))
        .collect(java.util.stream.Collectors.toSet());

      if (!partitions.isEmpty()) {
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = partitions.stream()
          .collect(java.util.stream.Collectors.toMap(
            partition -> partition,
            partition -> new OffsetAndMetadata(consumer.position(partition))
          ));
        consumer.commitSync(offsetsToCommit);
      }
    }
  }
}
