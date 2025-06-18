package org.folio.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.TestUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.caches.JobProfileSnapshotCache;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.folio.consumers.DataImportKafkaHandler.CHUNK_ID_HEADER;
import static org.folio.consumers.DataImportKafkaHandler.PROFILE_SNAPSHOT_ID_KEY;
import static org.folio.consumers.DataImportKafkaHandler.RECORD_ID_HEADER;
import static org.folio.consumers.DataImportKafkaHandler.USER_ID_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DataImportKafkaHandlerTest {

  private static final String TENANT_ID = "diku";
  private static final String OKAPI_URL = "http://localhost";
  private static final String KAFKA_ENV_ID = "test-env";
  private static final int KAFKA_MAX_REQUEST_SIZE_VAL = 1048576;

  private static Vertx vertx;
  private static KafkaContainer kafkaContainer = TestUtil.getKafkaContainer();
  private static KafkaConfig kafkaConfig;

  @Mock
  private JobProfileSnapshotCache profileSnapshotCacheMock;
  @Mock
  private EventHandler mockedEventHandler;
  private DataImportKafkaHandler dataImportKafkaHandler;
  private AutoCloseable mocksCloseable;

  private final ProfileSnapshotWrapper jobProfileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(new JobProfile()
        .withId(UUID.randomUUID().toString())).getMap())
    .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(ACTION_PROFILE)
      .withContent(JsonObject.mapFrom(new ActionProfile()
        .withAction(ActionProfile.Action.UPDATE)
        .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC)).getMap())));

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
    kafkaContainer.start();
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafkaContainer.getHost())
      .kafkaPort(String.valueOf(kafkaContainer.getFirstMappedPort()))
      .envId(KAFKA_ENV_ID)
      .maxRequestSize(KAFKA_MAX_REQUEST_SIZE_VAL)
      .build();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    vertx.close(context.asyncAssertSuccess(res -> kafkaContainer.stop()));
  }

  @Before
  public void setUp() {
    mocksCloseable = MockitoAnnotations.openMocks(this);
    when(profileSnapshotCacheMock.get(anyString(), any(OkapiConnectionParams.class)))
      .thenReturn(Future.succeededFuture(Optional.of(jobProfileSnapshotWrapper)));
    when(mockedEventHandler.isEligible(any(DataImportEventPayload.class)))
      .thenReturn(true);
    doAnswer(invocation -> CompletableFuture.completedFuture(invocation.<DataImportEventPayload>getArgument(0)))
      .when(mockedEventHandler).handle(any(DataImportEventPayload.class));
    dataImportKafkaHandler = new DataImportKafkaHandler(vertx, profileSnapshotCacheMock, kafkaConfig);
    EventManager.clearEventHandlers();
    EventManager.registerEventHandler(mockedEventHandler);
    EventManager.registerKafkaEventPublisher(kafkaConfig, vertx, 1);
  }

  @After
  public void tearDown() throws Exception {
    mocksCloseable.close();
  }

  @Test
  public void shouldHandleKafkaRecordAndPopulatePayloadWithHeaders(TestContext context) {
    // Given
    String expectedPermissions = JsonArray.of("test-permission").encode();
    String expectedUserId = UUID.randomUUID().toString();
    String expectedRecordId = UUID.randomUUID().toString();
    String expectedChunkId = UUID.randomUUID().toString();

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withCurrentNode(jobProfileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(EMPTY)
      .withContext(new HashMap<>(Map.of(
        PROFILE_SNAPSHOT_ID_KEY, jobProfileSnapshotWrapper.getId()
      )));

    Event event = new Event()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MATCHED.value())
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(eventPayload));

    List<KafkaHeader> headers = List.of(
      new KafkaHeaderImpl(PERMISSIONS, expectedPermissions),
      new KafkaHeaderImpl(USER_ID_HEADER, expectedUserId),
      new KafkaHeaderImpl(RECORD_ID_HEADER, expectedRecordId),
      new KafkaHeaderImpl(CHUNK_ID_HEADER, expectedChunkId));

    KafkaConsumerRecord<String, byte[]> kafkaRecord = mock(KafkaConsumerRecord.class);
    when(kafkaRecord.headers()).thenReturn(headers);
    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes());

    // When
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // Then
    future.onComplete(context.asyncAssertSuccess(v -> {
      ArgumentCaptor<DataImportEventPayload> payloadCaptor = ArgumentCaptor.forClass(DataImportEventPayload.class);
      verify(mockedEventHandler).handle(payloadCaptor.capture());
      DataImportEventPayload payload = payloadCaptor.getValue();
      context.assertEquals(expectedPermissions, payload.getContext().get(PERMISSIONS));
      context.assertEquals(expectedUserId, payload.getContext().get(USER_ID_HEADER));
      context.assertEquals(expectedRecordId, payload.getContext().get(RECORD_ID_HEADER));
      context.assertEquals(expectedChunkId, payload.getContext().get(CHUNK_ID_HEADER));
      verifyEventPublished(DI_COMPLETED.value(), context);
    }));
  }

  private void verifyEventPublished(String eventType, TestContext context) {
    String topicToObserve = formatToKafkaTopicName(eventType);
    List<ConsumerRecord<String, String>> observedEvents = observeKafkaEvents(topicToObserve);
    context.assertEquals(1, observedEvents.size());
  }

  private String formatToKafkaTopicName(String eventType) {
    return KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_ID, getDefaultNameSpace(), TENANT_ID, eventType);
  }

  private List<ConsumerRecord<String, String>> observeKafkaEvents(String topic) {
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    ConsumerRecords<String, String> records;
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      kafkaConsumer.subscribe(List.of(topic));
      records = kafkaConsumer.poll(Duration.ofSeconds(20));
    }
    return IteratorUtils.toList(records.iterator());
  }

}
