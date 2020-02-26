package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EventHandlersApiTest extends AbstractRestVerticleTest {


  public static final String HANDLERS_CREATED_INSTANCE_PATH = "/source-storage/handlers/created-inventory-instance";

  private JsonObject event = new JsonObject()
    .put("id", UUID.randomUUID().toString())
    .put("eventType", "DI_INVENTORY_INSTANCE_CREATED")
    .put("eventMetadata", new JsonObject()
      .put("tenantId", TENANT_ID)
      .put("eventTTL", 1)
      .put("publishedBy", "mod-inventory"));

  @Override
  public void clearTables(TestContext context) {
  }

  @Test
  public void shouldReturnEmptyListOnGetIfNoSnapshotsExist() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(event.encode())
      .post(HANDLERS_CREATED_INSTANCE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
  }
}
