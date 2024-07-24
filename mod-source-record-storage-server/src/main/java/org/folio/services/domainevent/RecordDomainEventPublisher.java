package org.folio.services.domainevent;

import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordDomainEventPublisher {

  public static final String RECORD_DOMAIN_TOPIC = "srs.source_records";
  public static final String SOURCE_RECORD_CREATED = "SOURCE_RECORD_CREATED";
  public static final String SOURCE_RECORD_UPDATED = "SOURCE_RECORD_UPDATED";
  private static final String RECORD_TYPE = "folio.srs.recordType";

  @Autowired
  private KafkaConfig kafkaConfig;

  public void publishRecordCreated(Record created, Map<String, String> okapiHeaders) {
    Vertx.vertx().executeBlocking(() -> {
      var kafkaHeaders = getKafkaHeaders(okapiHeaders, created.getRecordType());
      var key = created.getId();
      return sendEventToKafka(okapiHeaders.get(TENANT), Json.encode(created), SOURCE_RECORD_CREATED, kafkaHeaders,
        kafkaConfig, key);
    });
  }

  private List<KafkaHeader> getKafkaHeaders(Map<String, String> okapiHeaders, Record.RecordType recordType) {
    return new ArrayList<>(List.of(
      KafkaHeader.header(URL, okapiHeaders.get(URL)),
      KafkaHeader.header(TENANT, okapiHeaders.get(TENANT)),
      KafkaHeader.header(TOKEN, okapiHeaders.get(TOKEN)),
      KafkaHeader.header(RECORD_TYPE, recordType.value()))
    );
  }

}
