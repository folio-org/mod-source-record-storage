package org.folio.kafka;

import org.folio.rest.tools.PomReader;

import static java.lang.String.join;

public class KafkaTopicNameHelper {
  private static final String DEFAULT_NAMESPACE = "Default";

  private KafkaTopicNameHelper() {
    super();
  }

  public static String formatTopicName(String env, String nameSpace, String tenant, String eventType) {
    return join(".", env, nameSpace, tenant, eventType);
  }

  public static String formatGroupName(String eventType) {
    return join(".", eventType, getModuleName());
  }

  public static String formatSubscriptionPattern(String env, String nameSpace, String eventType) {
    return join("\\.", env, nameSpace, "\\w{4,}", eventType);
  }

  public static String getDefaultNameSpace() {
    return DEFAULT_NAMESPACE;
  }

  public static SubscriptionDefinition createSubscriptionDefinition(String env, String nameSpace, String eventType) {
    return SubscriptionDefinition.builder()
      .eventType(eventType)
      .subscriptionPattern(formatSubscriptionPattern(env, nameSpace, eventType))
      .build();
  }

  private static String getModuleName() {
    return PomReader.INSTANCE.getModuleName().replace("_", "-");
  }
}
