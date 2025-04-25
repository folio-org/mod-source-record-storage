package org.folio.services.domainevent;

import static java.util.Objects.isNull;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_CREATED;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_DELETED;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_UPDATED;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.MarcUtil;
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
    publishRecord(new DomainEventPayload(null, simplifyRecord(created)), okapiHeaders, SOURCE_RECORD_CREATED);
  }

  public void publishRecordUpdated(Record old, Record updated, Map<String, String> okapiHeaders) {
    publishRecord(new DomainEventPayload(simplifyRecord(old), simplifyRecord(updated)), okapiHeaders, SOURCE_RECORD_UPDATED);
  }

  public void publishRecordDeleted(Record deleted, Map<String, String> okapiHeaders) {
    publishRecord(new DomainEventPayload(simplifyRecord(deleted), null), okapiHeaders, SOURCE_RECORD_DELETED);
  }

  private void publishRecord(DomainEventPayload domainEventPayload, Map<String, String> okapiHeaders, SourceRecordDomainEventType eventType) {
    if (!domainEventsEnabled || notValidForPublishing(domainEventPayload)) {
      return;
    }
    try {
      Record aRecord = domainEventPayload.newRecord() != null ? domainEventPayload.newRecord() : domainEventPayload.oldRecord();
      var kafkaHeaders = getKafkaHeaders(okapiHeaders, aRecord.getRecordType());
      var key = aRecord.getId();
      var jsonContent = JsonObject.mapFrom(domainEventPayload);
      kafkaSender.sendEventToKafka(okapiHeaders.get(OKAPI_TENANT_HEADER), jsonContent.encode(),
        eventType.name(), kafkaHeaders, key);
    } catch (Exception e) {
      LOG.warn("Exception during Record domain event sending", e);
    }
  }

  private boolean notValidForPublishing(DomainEventPayload domainEventPayload) {
    if (domainEventPayload.newRecord() == null && domainEventPayload.oldRecord() == null) {
      LOG.warn("Old and new records are null and won't be sent as domain event");
      return true;
    }
    if (domainEventPayload.newRecord() != null && notValidRecord(domainEventPayload.newRecord())) {
      return true;
    }
    return domainEventPayload.oldRecord() != null && notValidRecord(domainEventPayload.oldRecord());
  }

  private static boolean notValidRecord(Record aRecord) {
    if (isNull(aRecord)) {
      LOG.warn("Record is null and won't be sent as domain event");
      return true;
    }
    if (isNull(aRecord.getRecordType())) {
      LOG.warn("Record [with id {}] contains no type information and won't be sent as domain event", aRecord.getId());
      return true;
    }
    if (isNull(aRecord.getParsedRecord())) {
      LOG.warn("Record [with id {}] contains no parsed record and won't be sent as domain event", aRecord.getId());
      return true;
    }
    if (isNull(aRecord.getParsedRecord().getContent())) {
      LOG.warn("Record [with id {}] contains no parsed record content and won't be sent as domain event",
        aRecord.getId());
      return true;
    }
    return false;
  }

  private List<KafkaHeader> getKafkaHeaders(Map<String, String> okapiHeaders, Record.RecordType recordType) {
    var headers = new ArrayList<KafkaHeader>();

    Optional.ofNullable(okapiHeaders.get(OKAPI_URL_HEADER))
      .ifPresent(url -> headers.add(KafkaHeader.header(OKAPI_URL_HEADER, url)));

    Optional.ofNullable(okapiHeaders.get(OKAPI_TENANT_HEADER))
      .ifPresent(tenant -> headers.add(KafkaHeader.header(OKAPI_TENANT_HEADER, tenant)));

    Optional.ofNullable(okapiHeaders.get(OKAPI_TOKEN_HEADER))
      .ifPresent(token -> headers.add(KafkaHeader.header(OKAPI_TOKEN_HEADER, token)));

    Optional.ofNullable(recordType)
      .map(Record.RecordType::value)
      .ifPresent(value -> headers.add(KafkaHeader.header(RECORD_TYPE, value)));
    return headers;
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private record DomainEventPayload(@JsonProperty("old") Record oldRecord, @JsonProperty("new") Record newRecord) {}

  private Record simplifyRecord(Record aRecord) {
    if (aRecord != null) {
      return MarcUtil.clone(aRecord, Record.class)
        .withErrorRecord(null)
        .withRawRecord(null);
    }
    return null;
  }
}
