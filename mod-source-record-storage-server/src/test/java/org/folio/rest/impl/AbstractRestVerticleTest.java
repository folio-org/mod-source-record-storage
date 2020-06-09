package org.folio.rest.impl;

import static org.folio.rest.impl.ModTenantAPI.LOAD_SAMPLE_PARAMETER;

import java.util.Collections;
import java.util.UUID;

import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import org.folio.dao.PostgresClientFactory;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public abstract class AbstractRestVerticleTest {

  static final String TENANT_ID = "diku";

  static final String SOURCE_STORAGE_RECORDS_PATH = "/lb-source-storage/records";
  static final String SOURCE_STORAGE_SNAPSHOTS_PATH = "/lb-source-storage/snapshots";
  static final String SOURCE_STORAGE_SOURCE_RECORDS_PATH = "/lb-source-storage/sourceRecords";

  static final String RAW_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/rawRecordContent.sample";
  static final String PARSED_RECORD_CONTENT_SAMPLE_PATH = "src/test/resources/parsedRecordContent.sample";
  static final String OKAPI_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkaWt1X2FkbWluIiwidXNlcl9pZCI6ImNjNWI3MzE3LWYyNDctNTYyMC1hYTJmLWM5ZjYxYjI5M2Q3NCIsImlhdCI6MTU3NzEyMTE4NywidGVuYW50IjoiZGlrdSJ9.0TDnGadsNpFfpsFGVLX9zep5_kIBJII2MU7JhkFrMRw";

  static Vertx vertx;
  static RequestSpecification spec;
  static RequestSpecification specWithoutUserId;

  private static String useExternalDatabase;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    int port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    String okapiUserId = UUID.randomUUID().toString();
    useExternalDatabase = System.getProperty(
      "org.folio.source.storage.test.database",
      "embedded");

    switch (useExternalDatabase) {
      case "environment":
        System.out.println("Using environment settings");
        break;
      case "external":
        String postgresConfigPath = System.getProperty(
          "org.folio.source.storage.test.config",
          "/postgres-conf-local.json");
        PostgresClient.setConfigFilePath(postgresConfigPath);
        break;
      case "embedded":
        PostgresClient.setIsEmbedded(true);
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.source.storage.test.database" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient(okapiUrl, "diku", "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, res -> {
      try {
        tenantClient.postTenant(new TenantAttributes()
          .withModuleTo("1.0")
          .withParameters(Collections.singletonList(new Parameter()
            .withKey(LOAD_SAMPLE_PARAMETER)
            .withValue("true"))), res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .setBaseUri(okapiUrl)
      .addHeader(RestVerticle.OKAPI_HEADER_TENANT, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, okapiUserId)
      .build();

    specWithoutUserId = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .setBaseUri(okapiUrl)
      .addHeader(RestVerticle.OKAPI_HEADER_TENANT, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_HEADER_TOKEN, OKAPI_TOKEN)
      .build();
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    PostgresClientFactory.closeAll();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        PostgresClient.stopEmbeddedPostgres();
      }
      async.complete();
    }));
  }

  @Before
  public abstract void clearTables(TestContext context);

}
