package org.folio.services.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.folio.kafka.KafkaConfig;
import org.junit.jupiter.api.Test;

class EventHandlingUtilTest {

  private static final String ENV = "env";
  private static final String EVENT = "event";
  private static final String TENANT = "tenant";

  @Test
  void shouldCreateTopicNameNoNamespace() {
    var kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    var expected = String.format("%s.%s.%s", ENV, TENANT, EVENT);
    var actual = EventHandlingUtil.createTopicNameNoNamespace(EVENT, TENANT, kafkaConfig);

    assertEquals(expected, actual);
  }

  @Test
  void shouldCreateSubscriptionPattern() {
    var expected = String.format("%s\\.\\w{1,}\\.%s", ENV, EVENT);
    var actual = EventHandlingUtil.createSubscriptionPattern(ENV, EVENT);

    assertEquals(expected, actual);
  }
}
