package org.folio.services;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.ClassRule;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;

public abstract class AbstractLBServiceTest {

  private static final String KAFKA_HOST = "FOLIO_KAFKA_HOST";
  private static final String KAFKA_PORT = "FOLIO_KAFKA_PORT";
  private static final String OKAPI_URL_ENV = "OKAPI_URL";
  private static final int PORT = NetworkUtils.nextFreePort();
  protected static final String OKAPI_URL = "http://localhost:" + PORT;

  static final String TENANT_ID = "diku";

  static PostgresClientFactory postgresClientFactory;

  static Vertx vertx;

//  @ClassRule
//  public static EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();

//    String[] hostAndPort = cluster.getBrokerList().split(":");
//    System.setProperty(KAFKA_HOST, hostAndPort[0]);
//    System.setProperty(KAFKA_PORT, hostAndPort[1]);
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);

    PostgresClient.setIsEmbedded(true);

    PostgresClient.getInstance(vertx).startEmbeddedPostgres();

    postgresClientFactory = new PostgresClientFactory(vertx);

    int port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    TenantClient tenantClient = new TenantClient(okapiUrl, "diku", "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, deployResponse -> {
      try {
        tenantClient.postTenant(new TenantAttributes().withModuleTo("3.2.0"), postTenantResponse -> {
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
      PostgresClient.stopEmbeddedPostgres();
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
