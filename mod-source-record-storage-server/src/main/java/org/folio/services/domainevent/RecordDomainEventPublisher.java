package org.folio.services.domainevent;

import static java.util.Objects.isNull;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_CREATED;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_UPDATED;

import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.kafka.KafkaSender;
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

  private void publishRecord(Record aRecord, Map<String, String> okapiHeaders, SourceRecordDomainEventType eventType) {
    if (!domainEventsEnabled || notValidForPublishing(aRecord)) {
      return;
    }
    try {
      var kafkaHeaders = getKafkaHeaders(okapiHeaders, aRecord.getRecordType());
      var key = aRecord.getId();
      var jsonContent = JsonObject.mapFrom(aRecord);
      kafkaSender.sendEventToKafka(okapiHeaders.get(OKAPI_TENANT_HEADER), jsonContent.encode(),
        eventType.name(), kafkaHeaders, key);
    } catch (Exception e) {
      LOG.error("Exception during Record domain event sending", e);
    }
  }

  private boolean notValidForPublishing(Record aRecord) {
    if (isNull(aRecord.getRecordType())) {
      LOG.error("Record [with id {}] contains no type information and won't be sent as domain event", aRecord.getId());
      return true;
    }
    if (isNull(aRecord.getParsedRecord())) {
      LOG.error("Record [with id {}] contains no parsed record and won't be sent as domain event", aRecord.getId());
      return true;
    }
    if (isNull(aRecord.getParsedRecord().getContent())) {
      LOG.error("Record [with id {}] contains no parsed record content and won't be sent as domain event",
        aRecord.getId());
      return true;
    }
    return false;
  }

  private List<KafkaHeader> getKafkaHeaders(Map<String, String> okapiHeaders, Record.RecordType recordType) {
    return List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, okapiHeaders.get(OKAPI_URL_HEADER)),
      KafkaHeader.header(OKAPI_TENANT_HEADER, okapiHeaders.get(OKAPI_TENANT_HEADER)),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, okapiHeaders.get(OKAPI_TOKEN_HEADER)),
      KafkaHeader.header(RECORD_TYPE, recordType.value())
    );
  }

}
