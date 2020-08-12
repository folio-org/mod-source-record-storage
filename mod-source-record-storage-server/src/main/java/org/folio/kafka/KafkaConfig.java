package org.folio.kafka;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.Map;

@Getter
@Builder
@ToString
public class KafkaConfig {
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET = "kafka.consumer.auto.offset.reset";
  public static final String KAFKA_CONSUMER_AUTO_OFFSET_RESET_DEFAULT = "earliest";

  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE = "kafka.consumer.metadata.max.age.ms";
  public static final String KAFKA_CONSUMER_METADATA_MAX_AGE_DEFAULT = "30000";

//  public static final String KAFKA_NUMBER_OF_PARTITIONS = "kafka.number.of.patitions";
  public static final String KAFKA_NUMBER_OF_PARTITIONS = "NUMBER_OF_PARTITIONS";
  public static final String KAFKA_NUMBER_OF_PARTITIONS_DEFAULT = "10";

  private final String kafkaHost;
  private final String kafkaPort;
  private final String okapiUrl;
  private final int replicationFactor;
  private final String envId;

  public Map<String, String> getProducerProps() {
    Map<String, String> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    return producerProps;
  }

  public Map<String, String> getConsumerProps() {
    Map<String, String> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, SimpleConfigurationReader.getValue(KAFKA_CONSUMER_AUTO_OFFSET_RESET, KAFKA_CONSUMER_AUTO_OFFSET_RESET_DEFAULT));
    consumerProps.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, SimpleConfigurationReader.getValue(KAFKA_CONSUMER_METADATA_MAX_AGE, KAFKA_CONSUMER_METADATA_MAX_AGE_DEFAULT));
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    return consumerProps;
  }

  public String getKafkaUrl() {
    return kafkaHost + ":" + kafkaPort;
  }

  public int getNumberOfPartitions() {
    return Integer.parseInt(SimpleConfigurationReader.getValue(KAFKA_NUMBER_OF_PARTITIONS, KAFKA_NUMBER_OF_PARTITIONS_DEFAULT));
  }


}
