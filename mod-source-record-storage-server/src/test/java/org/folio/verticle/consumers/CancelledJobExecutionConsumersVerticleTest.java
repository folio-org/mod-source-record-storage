package org.folio.verticle.consumers;

import static java.time.Duration.ofSeconds;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.folio.TestUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.headers.FolioKafkaHeaders;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.kafka.KafkaContainer;

@ExtendWith(VertxExtension.class)
public class CancelledJobExecutionConsumersVerticleTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV_ID = "test-env";
  private static final long CACHE_EXPIRATION_TIME_MINS = 5;

  private static KafkaContainer kafkaContainer = TestUtil.getKafkaContainer();
  private static Vertx vertx;
  private static KafkaConfig kafkaConfig;

  private CancelledJobsIdsCache cancelledJobsIdsCache;
  private String verticleDeploymentId;

  @BeforeAll
  static void beforeAll() {
    vertx = Vertx.vertx();
    kafkaContainer.start();
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafkaContainer.getHost())
      .kafkaPort(String.valueOf(kafkaContainer.getFirstMappedPort()))
      .envId(KAFKA_ENV_ID)
      .build();
  }

  @AfterAll
  static void afterAll() throws Exception {
    CompletableFuture<Void> close = new CompletableFuture<>();
    vertx.close().onComplete(v -> { kafkaContainer.stop(); close.complete(null); });
    close.get(30, TimeUnit.SECONDS);
  }

  @BeforeEach
  void setUp(VertxTestContext testContext) {
    cancelledJobsIdsCache = new CancelledJobsIdsCache(CACHE_EXPIRATION_TIME_MINS);
    deployVerticle(cancelledJobsIdsCache).onComplete(testContext.succeedingThenComplete());
  }

  @AfterEach
  void tearDown(VertxTestContext testContext) {
    undeployVerticle().onComplete(testContext.succeedingThenComplete());
  }

  @Test
  void shouldReadAndPutMultipleJobIdsToCache() throws ExecutionException, InterruptedException {
    List<String> ids = generateJobIds(100);

    sendJobIdsToKafka(ids);

    await().atMost(ofSeconds(3))
      .untilAsserted(() -> ids.forEach(id -> assertTrue(cancelledJobsIdsCache.contains(id))));
  }

  @Test
  void shouldReadAllEventsFromTopicIfVerticleWasRestarted(VertxTestContext testContext)
    throws ExecutionException, InterruptedException {

    List<String> idsBatch1 = generateJobIds(100);
    sendJobIdsToKafka(idsBatch1);
    await().atMost(ofSeconds(3))
      .untilAsserted(() -> idsBatch1.forEach(id -> assertTrue(cancelledJobsIdsCache.contains(id))));

    List<String> idsBatch2 = generateJobIds(200);

    undeployVerticle().onComplete(testContext.succeeding(v -> {
      try {
        sendJobIdsToKafka(idsBatch2);
      } catch (ExecutionException | InterruptedException e) {
        testContext.failNow(e);
        return;
      }

      cancelledJobsIdsCache = new CancelledJobsIdsCache(CACHE_EXPIRATION_TIME_MINS);
      deployVerticle(cancelledJobsIdsCache).onComplete(testContext.succeeding(id -> {
        await().atMost(ofSeconds(3))
          .untilAsserted(() -> idsBatch1.forEach(i -> assertTrue(cancelledJobsIdsCache.contains(i))));
        await().atMost(ofSeconds(3))
          .untilAsserted(() -> idsBatch2.forEach(i -> assertTrue(cancelledJobsIdsCache.contains(i))));
        testContext.completeNow();
      }));
    }));
  }

  private Future<String> deployVerticle(CancelledJobsIdsCache cancelledJobsIdsCache) {
    DeploymentOptions deploymentOptions = new DeploymentOptions()
      .setThreadingModel(ThreadingModel.WORKER)
      .setInstances(1);

    return vertx.deployVerticle(
      () -> new CancelledJobExecutionConsumersVerticle(cancelledJobsIdsCache, kafkaConfig, 1000),
      deploymentOptions
    ).onSuccess(deploymentId -> verticleDeploymentId = deploymentId);
  }

  private Future<Void> undeployVerticle() {
    return vertx.undeploy(verticleDeploymentId);
  }

  private List<String> generateJobIds(int idsNumber) {
    return Stream.iterate(0, i -> i < idsNumber, i -> ++i)
      .map(i -> UUID.randomUUID().toString())
      .toList();
  }

  private void sendJobIdsToKafka(List<String> ids) throws ExecutionException, InterruptedException {
    for (String id : ids) {
      Event event = new Event().withEventPayload(id);
      sendEvent(DI_JOB_CANCELLED.value(), Json.encode(event));
    }
  }

  private void sendEvent(String topic, String payload) throws ExecutionException, InterruptedException {
    try (KafkaProducer<String, String> kafkaProducer = createKafkaProducer()) {
      var topicName = formatToKafkaTopicName(topic);
      var producerRecord = new ProducerRecord<>(topicName, "test-key", payload);
      producerRecord.headers().add(FolioKafkaHeaders.TENANT_ID, TENANT_ID.getBytes());
      kafkaProducer.send(producerRecord).get();
    }
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProperties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(producerProperties);
  }

  private String formatToKafkaTopicName(String eventType) {
    return KafkaTopicNameHelper.formatTopicName(KAFKA_ENV_ID, getDefaultNameSpace(), TENANT_ID, eventType);
  }

}
