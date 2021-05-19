package org.folio.config;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import org.folio.kafka.KafkaConfig;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.kafka.cache.util.CacheUtil;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.dao",
  "org.folio.services"})
public class ApplicationConfig {

  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${KAFKA_HOST:kafka}")
  private String kafkaHost;
  @Value("${KAFKA_PORT:9092}")
  private String kafkaPort;
  @Value("${OKAPI_URL:http://okapi:9130}")
  private String okapiUrl;
  @Value("${REPLICATION_FACTOR:1}")
  private int replicationFactor;
  @Value("${ENV:folio}")
  private String envId;

  @Bean(name = "newKafkaConfig")
  public KafkaConfig kafkaConfigBean() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(envId)
      .kafkaHost(kafkaHost)
      .kafkaPort(kafkaPort)
      .okapiUrl(okapiUrl)
      .replicationFactor(replicationFactor)
      .build();
    LOGGER.debug("kafkaConfig: {}", kafkaConfig);

    return kafkaConfig;
  }

  @Bean
  public KafkaInternalCache kafkaInternalCache(KafkaConfig kafkaConfig, Vertx vertx,
                                               @Value("${srs.kafka.cache.cleanup.interval.ms:3600000}") long cleanupInterval,
                                               @Value("${srs.kafka.cache.expiration.time.hours:3}") int expirationTime) {
    KafkaInternalCache kafkaInternalCache = KafkaInternalCache.builder()
      .kafkaConfig(kafkaConfig)
      .build();

    kafkaInternalCache.initKafkaCache();
    CacheUtil.initCacheCleanupPeriodicTask(vertx, kafkaInternalCache, cleanupInterval, expirationTime);

    return kafkaInternalCache;
  }
}
