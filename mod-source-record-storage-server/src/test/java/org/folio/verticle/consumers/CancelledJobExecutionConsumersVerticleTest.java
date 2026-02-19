package org.folio.verticle.consumers;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.folio.TestUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.headers.FolioKafkaHeaders;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.caches.CancelledJobsIdsCache;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.kafka.KafkaContainer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.folio.DataImportEventTypes.DI_JOB_CANCELLED;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class CancelledJobExecutionConsumersVerticleTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV_ID = "test-env";
  private static final long CACHE_EXPIRATION_TIME_MINS = 5;

  private static KafkaContainer kafkaContainer = TestUtil.getKafkaContainer();
  private static Vertx vertx;
  private static KafkaConfig kafkaConfig;

  private CancelledJobsIdsCache cancelledJobsIdsCache;
  private String verticleDeploymentId;

  @BeforeClass
  public static void beforeAll() {
    vertx = Vertx.vertx();
    kafkaContainer.start();
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafkaContainer.getHost())
      .kafkaPort(String.valueOf(kafkaContainer.getFirstMappedPort()))
      .envId(KAFKA_ENV_ID)
      .build();
  }

  @AfterClass
  public static void afterAll(TestContext context) {
    vertx.close().onComplete(context.asyncAssertSuccess(res -> kafkaContainer.stop()));
  }

  @Before
  public void setUp(TestContext context) {
    cancelledJobsIdsCache = new CancelledJobsIdsCache(CACHE_EXPIRATION_TIME_MINS);
    deployVerticle(cancelledJobsIdsCache).onComplete(context.asyncAssertSuccess());
  }

  @After
  public void tearDown(TestContext context) {
    undeployVerticle().onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void shouldReadAndPutMultipleJobIdsToCache() throws ExecutionException, InterruptedException {
    List<String> ids = generateJobIds(100);

    sendJobIdsToKafka(ids);

    await().atMost(ofSeconds(3))
      .untilAsserted(() -> ids.forEach(id -> assertTrue(cancelledJobsIdsCache.contains(id))));
  }

  @Test
  public void shouldReadAllEventsFromTopicIfVerticleWasRestarted(TestContext context)
    throws ExecutionException, InterruptedException {

    List<String> idsBatch1 = generateJobIds(100);
    sendJobIdsToKafka(idsBatch1);
    await().atMost(ofSeconds(3))
      .untilAsserted(() -> idsBatch1.forEach(id -> assertTrue(cancelledJobsIdsCache.contains(id))));

    // stop currently deployed verticle
    Async async = context.async();
    undeployVerticle().onComplete(context.asyncAssertSuccess(v -> async.complete()));

    async.await(3000);
    List<String> idsBatch2 = generateJobIds(200);
    sendJobIdsToKafka(idsBatch2);

    // redeploy the verticle
    Async async2 = context.async();
    cancelledJobsIdsCache = new CancelledJobsIdsCache(CACHE_EXPIRATION_TIME_MINS);
    deployVerticle(cancelledJobsIdsCache)
      .onComplete(context.asyncAssertSuccess(v -> async2.complete()));

    async2.await(3000);
    await().atMost(ofSeconds(3))
      .untilAsserted(() -> idsBatch1.forEach(id -> assertTrue(cancelledJobsIdsCache.contains(id))));
    await().atMost(ofSeconds(3))
      .untilAsserted(() -> idsBatch2.forEach(id -> assertTrue(cancelledJobsIdsCache.contains(id))));
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
