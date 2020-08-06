package org.folio.kafka;

import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

public interface AsyncRecordHandler<K, V> {
  Future<K> handle(KafkaConsumerRecord<K, V> record);
}
