package org.folio.services.domainevent;

import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType.SOURCE_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType.SOURCE_RECORD_UPDATED;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.SourceRecordDomainEvent;
import org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordDomainEventPublisher {

  public static final String RECORD_DOMAIN_TOPIC = "srs.source_records";
  private static final String RECORD_TYPE = "folio.srs.recordType";
  private static final Logger LOG = LogManager.getLogger();

  @Autowired
  private KafkaConfig kafkaConfig;

  public void publishRecordCreated(Record created, Map<String, String> okapiHeaders) {
    publishRecord(created, okapiHeaders, SOURCE_RECORD_CREATED);
  }

  public void publishRecordUpdated(Record updated, Map<String, String> okapiHeaders) {
    publishRecord(updated, okapiHeaders, SOURCE_RECORD_UPDATED);
  }

  private void publishRecord(Record aRecord, Map<String, String> okapiHeaders, EventType eventType) {
    Vertx.vertx().executeBlocking(() -> {
      try {
        var kafkaHeaders = getKafkaHeaders(okapiHeaders, aRecord.getRecordType());
        var key = aRecord.getId();
        return sendEventToKafka(okapiHeaders.get(TENANT), getEvent(aRecord, eventType),
          eventType.value(), kafkaHeaders, kafkaConfig, key);
      } catch (Exception e) {
        LOG.error("Exception during Record domain event sending", e);
        return Future.failedFuture(e);
      }
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

  private String getEvent(Record eventRecord, EventType type) {
    var event = new SourceRecordDomainEvent()
      .withId(eventRecord.getId())
      .withEventType(type)
      .withEventPayload((String) eventRecord.getParsedRecord().getContent());
    return Json.encode(event);
  }

}
