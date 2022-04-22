package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.path.json.mapper.factory.Jackson2ObjectMapperFactory;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.folio.dao.PostgresClientFactory;
import org.folio.kafka.KafkaConfig;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.lang.reflect.Type;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;

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

  protected static final String RAW_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawEdifactRecordContent.sample";
  protected static final String PARSED_EDIFACT_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedEdifactRecordContent.sample";

  protected static Vertx vertx;
  protected static KafkaConfig kafkaConfig;

  protected static PostgresClientFactory postgresClientFactory;

  public static EmbeddedKafkaCluster cluster;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();

    cluster = provisionWith(defaultClusterConfig());
    cluster.start();
    String[] hostAndPort = cluster.getBrokerList().split(":");
    System.setProperty(KAFKA_HOST, hostAndPort[0]);
    System.setProperty(KAFKA_PORT, hostAndPort[1]);
    System.setProperty(KAFKA_ENV, KAFKA_ENV_ID);
    System.setProperty(KAFKA_MAX_REQUEST_SIZE, String.valueOf(KAFKA_MAX_REQUEST_SIZE_VAL));
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .envId(KAFKA_ENV_ID)
      .maxRequestSize(KAFKA_MAX_REQUEST_SIZE_VAL)
      .build();

    RestAssured.config = RestAssuredConfig.config().objectMapperConfig(new ObjectMapperConfig()
      .jackson2ObjectMapperFactory(new Jackson2ObjectMapperFactory() {
        @Override
        public ObjectMapper create(Type arg0, String arg1) {
          ObjectMapper objectMapper = new ObjectMapper();
          return objectMapper;
        }
      }
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

    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, deployResponse -> {
      try {
        tenantClient.postTenant(new TenantAttributes().withModuleTo("3.2.0"), res2 -> {
          postgresClientFactory = new PostgresClientFactory(vertx);
          if (res2.result().statusCode() == 204) {
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
        async.complete();
      }
    });
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    PostgresClientFactory.closeAll();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopPostgresTester();
      async.complete();
    }));
  }

  void compareMetadata(TestContext context, Metadata expected, Metadata actual) {
    context.assertEquals(expected.getCreatedByUserId(), actual.getCreatedByUserId());
    context.assertNotNull(actual.getCreatedDate());
    context.assertEquals(expected.getUpdatedByUserId(), actual.getUpdatedByUserId());
    context.assertNotNull(actual.getUpdatedDate());
  }

}
