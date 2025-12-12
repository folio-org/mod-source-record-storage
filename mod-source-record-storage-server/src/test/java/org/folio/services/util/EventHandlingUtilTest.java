package org.folio.services.util;

import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.DataImportEventPayload;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.folio.services.domainevent.RecordDomainEventPublisher.RECORD_DOMAIN_EVENT_TOPIC;
import static org.folio.services.util.EventHandlingUtil.OKAPI_REQUEST_HEADER;
import static org.folio.services.util.EventHandlingUtil.OKAPI_USER_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EventHandlingUtilTest {

  private static final String ENV = "env";
  private static final String EVENT = "event";
  private static final String TENANT = "tenant";
  private static final String OKAPI_URL = "http://localhost:9130";
  private static final String TOKEN = "test-token";
  private static final String USER_ID = "user-123";
  private static final String REQUEST_ID = "request-456";

  @Test
  public void shouldCreateSubscriptionPattern() {
    var expected = String.format("%s\\.\\w{1,}\\.%s", ENV, EVENT);
    var actual = EventHandlingUtil.createSubscriptionPattern(ENV, EVENT);

    assertEquals(expected, actual);
  }

  @Test
  public void shouldConstructModuleName() {
    // When
    String moduleName = EventHandlingUtil.constructModuleName();

    // Then
    assertNotNull(moduleName);
    assertFalse(moduleName.isEmpty());
  }

  @Test
  public void shouldCreateTopicNameForDomainEvent() {
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
  public void shouldCreateTopicNameForSourceRecordUpdatedDomainEvent() {
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
  public void shouldCreateTopicNameForSourceRecordDeletedDomainEvent() {
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
  public void shouldCreateTopicNameForRegularEvent() {
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
  public void shouldConvertDataImportEventPayloadToOkapiHeaders() {
    // Given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT)
      .withToken(TOKEN)
      .withContext(new HashMap<>());

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(eventPayload);

    // Then
    assertEquals(OKAPI_URL, headers.get(OKAPI_URL_HEADER));
    assertEquals(TENANT, headers.get(OKAPI_TENANT_HEADER));
    assertEquals(TOKEN, headers.get(OKAPI_TOKEN_HEADER));
    assertNull(headers.get(OKAPI_USER_HEADER));
    assertNull(headers.get(OKAPI_REQUEST_HEADER));
  }

  @Test
  public void shouldConvertDataImportEventPayloadToOkapiHeadersWithUserIdAndRequestId() {
    // Given
    HashMap<String, String> context = new HashMap<>();
    context.put(OKAPI_USER_HEADER, USER_ID);
    context.put(OKAPI_REQUEST_HEADER, REQUEST_ID);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT)
      .withToken(TOKEN)
      .withContext(context);

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(eventPayload);

    // Then
    assertEquals(OKAPI_URL, headers.get(OKAPI_URL_HEADER));
    assertEquals(TENANT, headers.get(OKAPI_TENANT_HEADER));
    assertEquals(TOKEN, headers.get(OKAPI_TOKEN_HEADER));
    assertEquals(USER_ID, headers.get(OKAPI_USER_HEADER));
    assertEquals(REQUEST_ID, headers.get(OKAPI_REQUEST_HEADER));
  }

  @Test
  public void shouldConvertKafkaHeadersToOkapiHeaders() {
    // Given
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders);

    // Then
    assertEquals(OKAPI_URL, headers.get(OKAPI_URL_HEADER));
    assertEquals(TENANT, headers.get(OKAPI_TENANT_HEADER));
    assertEquals(TOKEN, headers.get(OKAPI_TOKEN_HEADER));
    assertEquals(USER_ID, headers.get(OKAPI_USER_HEADER));
    assertEquals(REQUEST_ID, headers.get(OKAPI_REQUEST_HEADER));
  }

  @Test
  public void shouldConvertKafkaHeadersToOkapiHeadersWithoutOptionalHeaders() {
    // Given
    List<KafkaHeader> kafkaHeaders = List.of(
      KafkaHeader.header(OKAPI_URL_HEADER, OKAPI_URL),
      KafkaHeader.header(OKAPI_TENANT_HEADER, TENANT),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, TOKEN)
    );

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders);

    // Then
    assertEquals(OKAPI_URL, headers.get(OKAPI_URL_HEADER));
    assertEquals(TENANT, headers.get(OKAPI_TENANT_HEADER));
    assertEquals(TOKEN, headers.get(OKAPI_TOKEN_HEADER));
    assertNull(headers.get(OKAPI_USER_HEADER));
    assertNull(headers.get(OKAPI_REQUEST_HEADER));
  }

  @Test
  public void shouldConvertKafkaHeadersToOkapiHeadersWithTenantOverride() {
    // Given
    String overrideTenant = "override-tenant";
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders, overrideTenant);

    // Then
    assertEquals(OKAPI_URL, headers.get(OKAPI_URL_HEADER));
    assertEquals(overrideTenant, headers.get(OKAPI_TENANT_HEADER));
    assertEquals(TOKEN, headers.get(OKAPI_TOKEN_HEADER));
    assertEquals(USER_ID, headers.get(OKAPI_USER_HEADER));
    assertEquals(REQUEST_ID, headers.get(OKAPI_REQUEST_HEADER));
  }

  @Test
  public void shouldConvertKafkaHeadersToOkapiHeadersWithNullTenantOverride() {
    // Given
    List<KafkaHeader> kafkaHeaders = createKafkaHeaders();

    // When
    Map<String, String> headers = EventHandlingUtil.toOkapiHeaders(kafkaHeaders, null);

    // Then
    assertEquals(OKAPI_URL, headers.get(OKAPI_URL_HEADER));
    assertEquals(TENANT, headers.get(OKAPI_TENANT_HEADER));
    assertEquals(TOKEN, headers.get(OKAPI_TOKEN_HEADER));
    assertEquals(USER_ID, headers.get(OKAPI_USER_HEADER));
    assertEquals(REQUEST_ID, headers.get(OKAPI_REQUEST_HEADER));
  }

  @Test
  public void shouldCreateProducerRecordWithAllFields() {
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
  public void shouldCreateProducerRecordWithDomainEventType() {
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
      KafkaHeader.header(OKAPI_URL_HEADER, OKAPI_URL),
      KafkaHeader.header(OKAPI_TENANT_HEADER, TENANT),
      KafkaHeader.header(OKAPI_TOKEN_HEADER, TOKEN),
      KafkaHeader.header(OKAPI_USER_HEADER, USER_ID),
      KafkaHeader.header(OKAPI_REQUEST_HEADER, REQUEST_ID)
    );
  }
}
