package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.restassured.RestAssured;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.folio.TestUtil;
import org.folio.dao.PostgresClientFactory;
import org.folio.kafka.KafkaConfig;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.services.util.AdditionalFieldsUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.kafka.KafkaContainer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.services.util.AdditionalFieldsUtil.TAG_005;
import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;

public abstract class AbstractLBServiceTest {

  private static final String KAFKA_HOST = "KAFKA_HOST";
  private static final String KAFKA_PORT = "KAFKA_PORT";
  private static final String KAFKA_ENV = "ENV";
  private static final String KAFKA_ENV_ID = "test-env";
  private static final String KAFKA_MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE";
  private static final int KAFKA_MAX_REQUEST_SIZE_VAL = 1048576;
  private static final String OKAPI_URL_ENV = "OKAPI_URL";
  private static final int PORT = NetworkUtils.nextFreePort();

  protected static final String OKAPI_URL = "http://localhost:" + PORT;

  protected static final String TENANT_ID = "diku";
  protected static final String TOKEN = "dummy";

  protected static final String RAW_MARC_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawMarcRecordContent.sample";
  protected static final String PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedMarcRecordContent.sample";

  protected static final String PARSED_MARC_RECORD_CONTENT_SAMPLE_PATH_035_CHECK = "src/test/resources/parsedMarcRecordTest035Update.sample";

  protected static final String RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawEdifactRecordContent.sample";
  protected static final String PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedEdifactRecordContent.sample";

  protected static Vertx vertx;
  protected static KafkaConfig kafkaConfig;

  protected static PostgresClientFactory postgresClientFactory;

  public static KafkaContainer kafkaContainer = TestUtil.getKafkaContainer();
  private static KafkaProducer<String, String> kafkaProducer;
  public static WireMockServer wireMockServer;

  @BeforeClass
  public static void setUpClass(TestContext context) {
    Async async = context.async();
    vertx = Vertx.vertx();

    kafkaContainer.start();
    kafkaProducer = createKafkaProducer();

    wireMockServer = new WireMockServer(new WireMockConfiguration().dynamicPort());
    wireMockServer.start();

    //Env variables
    System.setProperty(KAFKA_HOST, kafkaContainer.getHost());
    System.setProperty(KAFKA_PORT, kafkaContainer.getFirstMappedPort() + "");
    System.setProperty(KAFKA_ENV, KAFKA_ENV_ID);
    System.setProperty(KAFKA_MAX_REQUEST_SIZE, String.valueOf(KAFKA_MAX_REQUEST_SIZE_VAL));
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(kafkaContainer.getHost())
      .kafkaPort(kafkaContainer.getFirstMappedPort() + "")
      .envId(KAFKA_ENV_ID)
      .maxRequestSize(KAFKA_MAX_REQUEST_SIZE_VAL)
      .build();

    RestAssured.config = RestAssuredConfig.config().objectMapperConfig(new ObjectMapperConfig()
      .jackson2ObjectMapperFactory((arg0, arg1) -> new ObjectMapper()
      ));

    PostgresClient.setPostgresTester(new PostgresTesterContainer());
    JsonObject pgClientConfig = PostgresClient.getInstance(vertx).getConnectionConfig();

    Envs.setEnv(
      pgClientConfig.getString(PostgresClientFactory.HOST),
      pgClientConfig.getInteger(PostgresClientFactory.PORT),
      pgClientConfig.getString(PostgresClientFactory.USERNAME),
      pgClientConfig.getString(PostgresClientFactory.PASSWORD),
      pgClientConfig.getString(PostgresClientFactory.DATABASE)
    );

    TenantClient tenantClient = new TenantClient(OKAPI_URL, TENANT_ID, TOKEN);
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", PORT));

    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions).onComplete( deployResponse -> {
      try {
        tenantClient.postTenant(new TenantAttributes().withModuleTo("%s-%s".formatted(ModuleName.getModuleName(), ModuleName.getModuleVersion())), res2 -> {
          postgresClientFactory = new PostgresClientFactory(vertx);
          context.assertTrue(res2.succeeded());
          if (res2.result().statusCode() == 204) {
            async.complete();
            return;
          }
          if (res2.result().statusCode() == 201) {
            tenantClient.getTenantByOperationId(res2.result().bodyAsJson(TenantJob.class).getId(), 60000, context.asyncAssertSuccess(res3 -> {
              context.assertTrue(res3.bodyAsJson(TenantJob.class).getComplete());
              String error = res3.bodyAsJson(TenantJob.class).getError();
              if (error != null) {
                context.assertTrue(error.contains("EventDescriptor was not registered for eventType"));
              }
            }));
          } else {
            context.assertEquals("Failed to make post tenant. Received status code 400", res2.result().bodyAsString());
          }
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
        context.fail(e);
      }
    });
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
//    Async async = context.async();
    PostgresClientFactory.closeAll();
    vertx.close().onComplete(context.asyncAssertSuccess(res -> {
      PostgresClient.stopPostgresTester();
      wireMockServer.stop();
      kafkaContainer.stop();
//      async.complete();
    }));
  }

  void compareMetadata(TestContext context, Metadata expected, Metadata actual) {
    context.assertEquals(expected.getCreatedByUserId(), actual.getCreatedByUserId());
    context.assertNotNull(actual.getCreatedDate());
    context.assertEquals(expected.getUpdatedByUserId(), actual.getUpdatedByUserId());
    context.assertNotNull(actual.getUpdatedDate());
  }

  protected String get005FieldExpectedDate() {
    return AdditionalFieldsUtil.dateTime005Formatter
      .format(ZonedDateTime.ofInstant(Instant.now(), ZoneId.systemDefault()));
  }

  protected void validate005Field(TestContext testContext, String expectedDate, Record record) {
    String actualDate = AdditionalFieldsUtil.getValueFromControlledField(record, TAG_005);
    testContext.assertEquals(expectedDate.substring(0, 10),
      actualDate.substring(0, 10));
  }

  protected void validate005Field(String expectedDate, Record record) {
    String actualDate = AdditionalFieldsUtil.getValueFromControlledField(record, TAG_005);
    assertEquals(expectedDate.substring(0, 10),
      actualDate.substring(0, 10));
  }

  private static KafkaProducer<String, String> createKafkaProducer() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new KafkaProducer<>(producerProperties);
  }

  @SneakyThrows
  protected RecordMetadata send(String topic, String key, String value, Map<String, String> headers) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
    headers.forEach((k, v) -> producerRecord.headers().add(k, v.getBytes(UTF_8)));
    return kafkaProducer.send(producerRecord).get();
  }

  protected ConsumerRecord<String, String> getKafkaEvent(String topic) {
    return getKafkaEvents(topic).get(0);
  }

  protected List<ConsumerRecord<String, String>> getKafkaEvents(String topic) {
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    ConsumerRecords<String, String> records;
    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      kafkaConsumer.subscribe(List.of(topic));
      records = kafkaConsumer.poll(Duration.ofSeconds(30));
    }
    return IteratorUtils.toList(records.iterator());
  }
}
