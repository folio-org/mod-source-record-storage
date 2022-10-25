package org.folio;

import org.folio.kafka.services.KafkaTopic;

public enum RecordStorageKafkaTopic implements KafkaTopic {
  TOPIC_ONE("marc-bib");

  private final String topic;

  RecordStorageKafkaTopic(String topic) {
    this.topic = topic;
  }

  @Override
  public String moduleName() {
    return "srs";
  }

  @Override
  public String topicName() {
    return topic;
  }
}
