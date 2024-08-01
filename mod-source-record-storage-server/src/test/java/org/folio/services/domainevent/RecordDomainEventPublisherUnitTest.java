package org.folio.services.domainevent;

import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType.SOURCE_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.SourceRecordDomainEvent.EventType.SOURCE_RECORD_UPDATED;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.kafka.KafkaSender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class RecordDomainEventPublisherUnitTest {

  @InjectMocks
  private RecordDomainEventPublisher publisher;
  @Mock
  private KafkaSender kafkaSender;

  @Test
  public void publishRecordCreated_shouldSendNoEvents_ifDomainEventsAreNotEnabled() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", false);
    var aRecord = new Record();
    var headers = Map.of(TENANT, "TENANT", URL, "OKAPI_URL", TOKEN, "TOKEN");

    // when
    publisher.publishRecordCreated(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordUpdated_shouldSendNoEvents_ifDomainEventsAreNotEnabled() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", false);
    var aRecord = new Record();
    var headers = Map.of(TENANT, "TENANT", URL, "OKAPI_URL", TOKEN, "TOKEN");

    // when
    publisher.publishRecordUpdated(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordCreated_shouldSendNoEvents_ifRecordHasNoType() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record();
    var headers = Map.of(TENANT, "TENANT", URL, "OKAPI_URL", TOKEN, "TOKEN");

    // when
    publisher.publishRecordCreated(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordUpdated_shouldSendNoEvents_ifRecordHasNoType() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record();
    var headers = Map.of(TENANT, "TENANT", URL, "OKAPI_URL", TOKEN, "TOKEN");

    // when
    publisher.publishRecordUpdated(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordCreated_shouldSendNoEvents_ifRecordContainsNoParsedContent() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record().withRecordType(Record.RecordType.MARC_BIB);
    var headers = Map.of(TENANT, "TENANT", URL, "OKAPI_URL", TOKEN, "TOKEN");

    // when
    publisher.publishRecordCreated(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordUpdated_shouldSendNoEvents_ifRecordContainsNoParsedContent() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record().withRecordType(Record.RecordType.MARC_BIB);
    var headers = Map.of(TENANT, "TENANT", URL, "OKAPI_URL", TOKEN, "TOKEN");

    // when
    publisher.publishRecordUpdated(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordCreated_shouldSendEvent_ifRecordIsValid() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var rawContent = "rawContent";
    var aRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(new RawRecord().withContent(rawContent));
    var tenantId = "TENANT";
    var okapiUrl = "OKAPI_URL";
    var token = "TOKEN";
    var givenHeaders = Map.of(TENANT, tenantId, URL, okapiUrl, TOKEN, token);
    var expectedHeaders = getKafkaHeaders(okapiUrl, tenantId, token, aRecord);
    var eventType = SOURCE_RECORD_CREATED.value();
    var expectedPayload = "{"
      + "\"id\":\"" + aRecord.getId() + "\""
      + ",\"eventType\":\"" + eventType + "\""
      + ",\"eventPayload\":\"" + rawContent + "\""
      + "}";

    // when
    publisher.publishRecordCreated(aRecord, givenHeaders);

    // then
    verify(kafkaSender).sendEventToKafka(tenantId, expectedPayload, eventType, expectedHeaders,
      aRecord.getId());
  }

  @Test
  public void publishRecordUpdated_shouldSendEvent_ifRecordIsValid() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var rawContent = "rawContent";
    var aRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withRawRecord(new RawRecord().withContent(rawContent));
    var tenantId = "TENANT";
    var okapiUrl = "OKAPI_URL";
    var token = "TOKEN";
    var givenHeaders = Map.of(TENANT, tenantId, URL, okapiUrl, TOKEN, token);
    var expectedHeaders = getKafkaHeaders(okapiUrl, tenantId, token, aRecord);
    var eventType = SOURCE_RECORD_UPDATED.value();
    var expectedPayload = "{"
      + "\"id\":\"" + aRecord.getId() + "\""
      + ",\"eventType\":\"" + eventType + "\""
      + ",\"eventPayload\":\"" + rawContent + "\""
      + "}";

    // when
    publisher.publishRecordUpdated(aRecord, givenHeaders);

    // thenÏ
    verify(kafkaSender).sendEventToKafka(tenantId, expectedPayload, eventType, expectedHeaders,
      aRecord.getId());
  }

  private List<KafkaHeader> getKafkaHeaders(String okapiUrl, String tenantId, String token, Record aRecord) {
    return List.of(
      KafkaHeader.header(URL, okapiUrl),
      KafkaHeader.header(TENANT, tenantId),
      KafkaHeader.header(TOKEN, token),
      KafkaHeader.header("folio.srs.recordType", aRecord.getRecordType().value())
    );
  }
}
