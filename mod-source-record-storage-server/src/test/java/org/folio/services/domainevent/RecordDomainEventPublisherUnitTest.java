package org.folio.services.domainevent;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_CREATED;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_DELETED;
import static org.folio.services.domainevent.SourceRecordDomainEventType.SOURCE_RECORD_UPDATED;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.folio.dao.util.MarcUtil;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ParsedRecord;
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
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

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
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordUpdated(aRecord, aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordDeleted_shouldSendNoEvents_ifDomainEventsAreNotEnabled() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", false);
    var aRecord = new Record();
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordDeleted(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordCreated_shouldSendNoEvents_ifRecordHasNoType() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record();
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

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
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordUpdated(aRecord, aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordDeleted_shouldSendNoEvents_ifRecordHasNoType() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record();
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordDeleted(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordCreated_shouldSendNoEvents_ifRecordContainsNoParsedContent() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record().withRecordType(Record.RecordType.MARC_BIB);
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

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
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordUpdated(aRecord, aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordDeleted_shouldSendNoEvents_ifRecordContainsNoParsedContent() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var aRecord = new Record().withRecordType(Record.RecordType.MARC_BIB);
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordDeleted(aRecord, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordUpdated_shouldSendNoEvents_ifOldAndNewRecordsAreNull() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var headers = Map.of(OKAPI_TENANT_HEADER, "TENANT", OKAPI_URL_HEADER, "OKAPI_URL", OKAPI_TOKEN_HEADER, "TOKEN");

    // when
    publisher.publishRecordUpdated(null, null, headers);

    // then
    verifyNoInteractions(kafkaSender);
  }

  @Test
  public void publishRecordCreated_shouldSendEvent_ifRecordIsValid() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var parsedContent = "parsedContent";
    var metadata = new Metadata()
      .withCreatedByUserId("createdByUserId")
      .withCreatedByUsername("createdByUsername")
      .withCreatedDate(new Date(10000L))
      .withUpdatedByUserId("updatedByUserId")
      .withUpdatedByUsername("updatedByUsername")
      .withUpdatedDate(new Date(20000L));
    var aRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withErrorRecord(new ErrorRecord())
      .withRawRecord(new RawRecord())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withMetadata(metadata);
    var expectedOldRecord = MarcUtil.clone(aRecord, Record.class).withErrorRecord(null).withRawRecord(null);

    var tenantId = "OKAPI_TENANT_HEADER";
    var okapiUrl = "OKAPI_URL";
    var token = "TOKEN";
    var givenHeaders = Map.of(OKAPI_TENANT_HEADER, tenantId, OKAPI_URL_HEADER, okapiUrl, OKAPI_TOKEN_HEADER, token);
    var expectedHeaders = getKafkaHeaders(okapiUrl, tenantId, token, aRecord);
    var eventType = SOURCE_RECORD_CREATED.name();
    var expectedPayload = new JsonObject().put("new", JsonObject.mapFrom(expectedOldRecord)).encode();

    // when
    publisher.publishRecordCreated(aRecord, givenHeaders);

    // then
    verify(kafkaSender).sendEventToKafka(tenantId, expectedPayload, eventType, expectedHeaders, aRecord.getId());
  }

  @Test
  public void publishRecordUpdated_shouldSendEvent_ifRecordIsValid() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var parsedContent = "parsedContent";
    var parsedContentUpdated = "parsedContentUpdated";
    var metadata = new Metadata()
      .withCreatedByUserId("createdByUserId")
      .withCreatedByUsername("createdByUsername")
      .withCreatedDate(new Date(10000L))
      .withUpdatedByUserId("updatedByUserId")
      .withUpdatedByUsername("updatedByUsername")
      .withUpdatedDate(new Date(20000L));
    var oldRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withErrorRecord(new ErrorRecord())
      .withRawRecord(new RawRecord())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withMetadata(metadata);

    var newRecord = MarcUtil.clone(oldRecord, Record.class).withParsedRecord(new ParsedRecord().withContent(parsedContentUpdated));
    var expectedOldRecord = MarcUtil.clone(oldRecord, Record.class).withErrorRecord(null).withRawRecord(null);
    var expectedNewRecord = MarcUtil.clone(newRecord, Record.class).withErrorRecord(null).withRawRecord(null);

    var tenantId = "TENANT";
    var okapiUrl = "OKAPI_URL";
    var token = "TOKEN";
    var givenHeaders = Map.of(OKAPI_TENANT_HEADER, tenantId, OKAPI_URL_HEADER, okapiUrl, OKAPI_TOKEN_HEADER, token);
    var expectedHeaders = getKafkaHeaders(okapiUrl, tenantId, token, oldRecord);
    var eventType = SOURCE_RECORD_UPDATED.name();
    var expectedPayload = new JsonObject()
      .put("old", JsonObject.mapFrom(expectedOldRecord))
      .put("new", JsonObject.mapFrom(expectedNewRecord)).encode();

    // when
    publisher.publishRecordUpdated(oldRecord, newRecord, givenHeaders);

    // then
    verify(kafkaSender).sendEventToKafka(tenantId, expectedPayload, eventType, expectedHeaders, newRecord.getId());
  }

  @Test
  public void publishRecordDeleted_shouldSendEvent_ifRecordIsValid() {
    // given
    ReflectionTestUtils.setField(publisher, "domainEventsEnabled", true);
    var parsedContent = "parsedContent";
    var metadata = new Metadata()
      .withCreatedByUserId("createdByUserId")
      .withCreatedByUsername("createdByUsername")
      .withCreatedDate(new Date(10000L))
      .withUpdatedByUserId("updatedByUserId")
      .withUpdatedByUsername("updatedByUsername")
      .withUpdatedDate(new Date(20000L));
    var aRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withErrorRecord(new ErrorRecord())
      .withRawRecord(new RawRecord())
      .withRecordType(Record.RecordType.MARC_BIB)
      .withParsedRecord(new ParsedRecord().withContent(parsedContent))
      .withMetadata(metadata);
    var expectedDeletedRecord = MarcUtil.clone(aRecord, Record.class).withErrorRecord(null).withRawRecord(null);

    var tenantId = "OKAPI_TENANT_HEADER";
    var okapiUrl = "OKAPI_URL";
    var token = "TOKEN";
    var givenHeaders = Map.of(OKAPI_TENANT_HEADER, tenantId, OKAPI_URL_HEADER, okapiUrl, OKAPI_TOKEN_HEADER, token);
    var expectedHeaders = getKafkaHeaders(okapiUrl, tenantId, token, aRecord);
    var eventType = SOURCE_RECORD_DELETED.name();
    var expectedPayload = new JsonObject().put("old", JsonObject.mapFrom(expectedDeletedRecord)).encode();

    // when
    publisher.publishRecordDeleted(aRecord, givenHeaders);

    // then
    verify(kafkaSender).sendEventToKafka(tenantId, expectedPayload, eventType, expectedHeaders, aRecord.getId());
  }

  private List<KafkaHeader> getKafkaHeaders(String okapiUrl, String tenantId, String token, Record aRecord) {
    return List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, okapiUrl),
      KafkaHeader.header(OKAPI_TENANT_HEADER, tenantId),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, token),
      KafkaHeader.header("folio.srs.recordType", aRecord.getRecordType().value())
    );
  }
}
