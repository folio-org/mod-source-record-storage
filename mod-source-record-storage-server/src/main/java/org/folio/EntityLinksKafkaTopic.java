package org.folio;


import org.folio.kafka.services.KafkaTopic;

public enum EntityLinksKafkaTopic implements KafkaTopic {
  LINKS_STATS("instance-authority-stats"),
  INSTANCE_AUTHORITY("instance-authority");

  private final String topic;

  EntityLinksKafkaTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String moduleName() {
    return "links";
  }

  @Override
  public String topicName() {
    return topic;
  }
}
