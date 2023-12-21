package org.folio;


import org.folio.kafka.services.KafkaTopic;

public enum AuthorityDomainKafkaTopic implements KafkaTopic {

  AUTHORITY("authority");

  private static final String AUTHORITIES_PREFIX = "authorities";
  private final String topic;

  AuthorityDomainKafkaTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String moduleName() {
    return AUTHORITIES_PREFIX;
  }

  @Override
  public String topicName() {
    return topic;
  }
}
