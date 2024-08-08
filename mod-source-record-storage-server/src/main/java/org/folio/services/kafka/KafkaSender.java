package org.folio.services.kafka;

import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import org.folio.kafka.KafkaConfig;
import org.folio.services.util.EventHandlingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaSender {

  @Autowired
  private KafkaConfig kafkaConfig;

  public Future<Boolean> sendEventToKafka(String tenantId, String eventPayload, String eventType,
                                          List<KafkaHeader> kafkaHeaders, String key) {
    return EventHandlingUtil.sendEventToKafka(tenantId, eventPayload, eventType, kafkaHeaders, kafkaConfig, key);
  }
}
