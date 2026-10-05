package org.folio.services.util;

import static org.folio.services.domainevent.RecordDomainEventPublisher.RECORD_DOMAIN_EVENT_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.Test;

public class EventHandlingUtilTest {

  private static final String ENV = "env";
  private static final String EVENT = "event";
  private static final String TENANT = "tenant";
  private static final String OKAPI_URL = "http://localhost:9130";
  private static final String TOKEN = "test-token";
  private static final String USER_ID = "user-123";
  private static final String REQUEST_ID = "request-456";

  @Test
  void shouldCreateSubscriptionPattern() {
    var expected = String.format("%s\\.\\w{1,}\\.%s", ENV, EVENT);
    var actual = EventHandlingUtil.createSubscriptionPattern(ENV, EVENT);

    assertEquals(expected, actual);
  }

  @Test
  void shouldConstructModuleName() {
    // When
    String moduleName = EventHandlingUtil.constructModuleName();

    // Then
    assertNotNull(moduleName);
    assertFalse(moduleName.isEmpty());
  }

  @Test
  void shouldCreateTopicNameForDomainEvent() {
    // Given
    String eventType = "SOURCE_RECORD_CREATED";
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    // When
    String topicName = EventHandlingUtil.createTopicName(eventType, TENANT, kafkaConfig);

    // Then
    String expected = KafkaTopicNameHelper.formatTopicName(ENV, TENANT, RECORD_DOMAIN_EVENT_TOPIC);
    assertEquals(expected, topicName);
  }

  @Test
  void shouldCreateTopicNameForSourceRecordUpdatedDomainEvent() {
    // Given
    String eventType = "SOURCE_RECORD_UPDATED";
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    // When
    String topicName = EventHandlingUtil.createTopicName(eventType, TENANT, kafkaConfig);

    // Then
    String expected = KafkaTopicNameHelper.formatTopicName(ENV, TENANT, RECORD_DOMAIN_EVENT_TOPIC);
    assertEquals(expected, topicName);
  }

  @Test
  void shouldCreateTopicNameForSourceRecordDeletedDomainEvent() {
    // Given
    String eventType = "SOURCE_RECORD_DELETED";
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    // When
    String topicName = EventHandlingUtil.createTopicName(eventType, TENANT, kafkaConfig);

    // Then
    String expected = KafkaTopicNameHelper.formatTopicName(ENV, TENANT, RECORD_DOMAIN_EVENT_TOPIC);
    assertEquals(expected, topicName);
  }

  @Test
  void shouldCreateTopicNameForRegularEvent() {
    // Given
    String eventType = "DI_COMPLETED";
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    // When
    String topicName = EventHandlingUtil.createTopicName(eventType, TENANT, kafkaConfig);

    // Then
    String expected = KafkaTopicNameHelper.formatTopicName(ENV, KafkaTopicNameHelper.getDefaultNameSpace(),
      TENANT, eventType);
    assertEquals(expected, topicName);
  }

  @Test
  void shouldConvertDataImportEventPayloadToOkapiHeaders() {
    // Given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT)
      .withToken(TOKEN)
      .withContext(new HashMap<>());

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(eventPayload);

    // Then
    assertEquals(OKAPI_URL, headers.get(XOkapiHeaders.URL));
    assertEquals(TENANT, headers.get(XOkapiHeaders.TENANT));
    assertEquals(TOKEN, headers.get(XOkapiHeaders.TOKEN));
    assertNull(headers.get(XOkapiHeaders.USER_ID));
    assertNull(headers.get(XOkapiHeaders.REQUEST_ID));
  }

  @Test
  void shouldConvertDataImportEventPayloadToOkapiHeadersWithUserIdAndRequestId() {
    // Given
    HashMap<String, String> context = new HashMap<>();
    context.put(XOkapiHeaders.USER_ID, USER_ID);
    context.put(XOkapiHeaders.REQUEST_ID, REQUEST_ID);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT)
      .withToken(TOKEN)
      .withContext(context);

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(eventPayload);

    // Then
    assertEquals(OKAPI_URL, headers.get(XOkapiHeaders.URL));
    assertEquals(TENANT, headers.get(XOkapiHeaders.TENANT));
    assertEquals(TOKEN, headers.get(XOkapiHeaders.TOKEN));
    assertEquals(USER_ID, headers.get(XOkapiHeaders.USER_ID));
    assertEquals(REQUEST_ID, headers.get(XOkapiHeaders.REQUEST_ID));
  }

  @Test
  void shouldConvertKafkaHeadersToOkapiHeaders() {
    // Given
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders);

    // Then
    assertEquals(OKAPI_URL, headers.get(XOkapiHeaders.URL));
    assertEquals(TENANT, headers.get(XOkapiHeaders.TENANT));
    assertEquals(TOKEN, headers.get(XOkapiHeaders.TOKEN));
    assertEquals(USER_ID, headers.get(XOkapiHeaders.USER_ID));
    assertEquals(REQUEST_ID, headers.get(XOkapiHeaders.REQUEST_ID));
  }

  @Test
  void shouldConvertKafkaHeadersToOkapiHeadersWithoutOptionalHeaders() {
    // Given
    List<KafkaHeader> kafkaHeaders = List.of(
      KafkaHeader.header(XOkapiHeaders.URL, OKAPI_URL),
      KafkaHeader.header(XOkapiHeaders.TENANT, TENANT),
      KafkaHeader.header(XOkapiHeaders.TOKEN, TOKEN)
    );

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders);

    // Then
    assertEquals(OKAPI_URL, headers.get(XOkapiHeaders.URL));
    assertEquals(TENANT, headers.get(XOkapiHeaders.TENANT));
    assertEquals(TOKEN, headers.get(XOkapiHeaders.TOKEN));
    assertNull(headers.get(XOkapiHeaders.USER_ID));
    assertNull(headers.get(XOkapiHeaders.REQUEST_ID));
  }

  @Test
  void shouldConvertKafkaHeadersToOkapiHeadersWithTenantOverride() {
    // Given
    String overrideTenant = "override-tenant";
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders, overrideTenant);

    // Then
    assertEquals(OKAPI_URL, headers.get(XOkapiHeaders.URL));
    assertEquals(overrideTenant, headers.get(XOkapiHeaders.TENANT));
    assertEquals(TOKEN, headers.get(XOkapiHeaders.TOKEN));
    assertEquals(USER_ID, headers.get(XOkapiHeaders.USER_ID));
    assertEquals(REQUEST_ID, headers.get(XOkapiHeaders.REQUEST_ID));
  }

  @Test
  void shouldConvertKafkaHeadersToOkapiHeadersWithNullTenantOverride() {
    // Given
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders, null);

    // Then
    assertEquals(OKAPI_URL, headers.get(XOkapiHeaders.URL));
    assertEquals(TENANT, headers.get(XOkapiHeaders.TENANT));
    assertEquals(TOKEN, headers.get(XOkapiHeaders.TOKEN));
    assertEquals(USER_ID, headers.get(XOkapiHeaders.USER_ID));
    assertEquals(REQUEST_ID, headers.get(XOkapiHeaders.REQUEST_ID));
  }

  @Test
  void shouldCreateProducerRecordWithAllFields() {
    // Given
    String eventPayload = "{\"test\":\"data\"}";
    String eventType = "TEST_EVENT";
    String key = "test-key";
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    // When
    var producerRecord = EventHandlingUtil.createProducerRecord(
      eventPayload, eventType, key, TENANT, kafkaHeaders, kafkaConfig);

    // Then
    assertNotNull(producerRecord);
    assertEquals(key, producerRecord.key());
    assertNotNull(producerRecord.value());
    assertNotNull(producerRecord.topic());

    // Verify the event value contains expected data (serialized as JSON string)
    String value = producerRecord.value();
    assertTrue(value.contains(eventType));
    assertTrue(value.contains(TENANT));
    assertTrue(value.contains("\"eventTTL\":1"));
  }

  @Test
  void shouldCreateProducerRecordWithDomainEventType() {
    // Given
    String eventPayload = "{\"recordId\":\"123\"}";
    String eventType = "SOURCE_RECORD_CREATED";
    String key = "record-123";
    List<KafkaHeader> kafkaHeaders = new ArrayList<>();
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(ENV)
      .build();

    // When
    var producerRecord = EventHandlingUtil.createProducerRecord(
      eventPayload, eventType, key, TENANT, kafkaHeaders, kafkaConfig);

    // Then
    assertNotNull(producerRecord);
    String expectedTopic = KafkaTopicNameHelper.formatTopicName(ENV, TENANT, RECORD_DOMAIN_EVENT_TOPIC);
    assertEquals(expectedTopic, producerRecord.topic());
  }

  private List<KafkaHeader> createKafkaHeaders() {
    return List.of(
      KafkaHeader.header(XOkapiHeaders.URL, OKAPI_URL),
      KafkaHeader.header(XOkapiHeaders.TENANT, TENANT),
      KafkaHeader.header(XOkapiHeaders.TOKEN, TOKEN),
      KafkaHeader.header(XOkapiHeaders.USER_ID, USER_ID),
      KafkaHeader.header(XOkapiHeaders.REQUEST_ID, REQUEST_ID)
    );
  }
}
