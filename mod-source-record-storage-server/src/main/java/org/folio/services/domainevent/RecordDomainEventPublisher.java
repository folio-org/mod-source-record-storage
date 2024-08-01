package org.folio.services.domainevent;

import static java.util.Objects.isNull;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType.SOURCE_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType.SOURCE_RECORD_UPDATED;

import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.services.kafka.KafkaSender;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.SourceRecordDomainEvent;
import org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RecordDomainEventPublisher {

  public static final String RECORD_DOMAIN_EVENT_TOPIC = "srs.source_records";
  private static final String RECORD_TYPE = "folio.srs.recordType";
  private static final Logger LOG = LogManager.getLogger();
  @Value("${DOMAIN_EVENTS_ENABLED:true}")
  private boolean domainEventsEnabled;
  @Autowired
  private KafkaSender kafkaSender;

  public void publishRecordCreated(Record created, Map<String, String> okapiHeaders) {
    publishRecord(created, okapiHeaders, SOURCE_RECORD_CREATED);
  }

  public void publishRecordUpdated(Record updated, Map<String, String> okapiHeaders) {
    publishRecord(updated, okapiHeaders, SOURCE_RECORD_UPDATED);
  }

  private void publishRecord(Record aRecord, Map<String, String> okapiHeaders, EventType eventType) {
    if (!domainEventsEnabled || notValidForPublishing(aRecord)) {
      return;
    }
    try {
      var kafkaHeaders = getKafkaHeaders(okapiHeaders, aRecord.getRecordType());
      var key = aRecord.getId();
      kafkaSender.sendEventToKafka(okapiHeaders.get(TENANT), getEvent(aRecord, eventType), eventType.value(),
        kafkaHeaders, key);
    } catch (Exception e) {
      LOG.error("Exception during Record domain event sending", e);
    }
  }

  private boolean notValidForPublishing(Record aRecord) {
    if (isNull(aRecord.getRecordType())) {
      LOG.error("Record [with id {}] contains no type information and won't be sent as domain event", aRecord.getId());
      return true;
    }
    if (isNull(aRecord.getRawRecord())) {
      LOG.error("Record [with id {}] contains no raw record and won't be sent as domain event", aRecord.getId());
      return true;
    }
    if (isNull(aRecord.getRawRecord().getContent())) {
      LOG.error("Record [with id {}] contains no raw record content and won't be sent as domain event",
        aRecord.getId());
      return true;
    }
    return false;
  }

  private List<KafkaHeader> getKafkaHeaders(Map<String, String> okapiHeaders, Record.RecordType recordType) {
    return List.of(
      KafkaHeader.header(URL, okapiHeaders.get(URL)),
      KafkaHeader.header(TENANT, okapiHeaders.get(TENANT)),
      KafkaHeader.header(TOKEN, okapiHeaders.get(TOKEN)),
      KafkaHeader.header(RECORD_TYPE, recordType.value())
    );
  }

  private String getEvent(Record eventRecord, EventType type) {
    var event = new SourceRecordDomainEvent()
      .withId(eventRecord.getId())
      .withEventType(type)
      .withEventPayload(eventRecord.getRawRecord().getContent());
    return Json.encode(event);
  }

}
