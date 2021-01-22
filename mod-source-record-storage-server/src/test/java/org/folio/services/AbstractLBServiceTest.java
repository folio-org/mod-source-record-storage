package org.folio.services;

import org.folio.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.Envs;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.PostgreSQLContainer;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractLBServiceTest {

  private static PostgreSQLContainer<?> postgresSQLContainer;

  static final String TENANT_ID = "diku";

  static PostgresClientFactory postgresClientFactory;

  static Vertx vertx;

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();

    String postgresImage = PomReader.INSTANCE.getProps().getProperty("postgres.image");
    postgresSQLContainer = new PostgreSQLContainer<>(postgresImage);
    postgresSQLContainer.start();

    Envs.setEnv(
      postgresSQLContainer.getHost(),
      postgresSQLContainer.getFirstMappedPort(),
      postgresSQLContainer.getUsername(),
      postgresSQLContainer.getPassword(),
      postgresSQLContainer.getDatabaseName()
    );

    int port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    TenantClient tenantClient = new TenantClient(okapiUrl, "diku", "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, deployResponse -> {
      try {
        tenantClient.postTenant(new TenantAttributes().withModuleTo("3.2.0"), postTenantResponse -> {
          postgresClientFactory = new PostgresClientFactory(vertx);
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    PostgresClientFactory.closeAll();
    vertx.close(context.asyncAssertSuccess(res -> {
      postgresSQLContainer.stop();
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
