package org.folio.rest.impl;

import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.processing.events.utils.ZIPArchiver;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.UUID;

@RunWith(VertxUnitRunner.class)
public class EventHandlersApiTest extends AbstractRestVerticleTest {

  public static final String HANDLERS_INSTANCE_PATH = "/source-storage/handlers/inventory-instance";
  public static final String HANDLERS_UPDATED_RECORD_PATH = "/source-storage/handlers/updated-record";

  private JsonObject event = new JsonObject()
    .put("id", UUID.randomUUID().toString())
    .put("eventMetadata", new JsonObject()
      .put("tenantId", TENANT_ID)
      .put("eventTTL", 1)
      .put("publishedBy", "mod-inventory"))
    .put("context", new JsonObject());

  @Override
  public void clearTables(TestContext context) {
    // do nothing
  }

  @Test
  public void shouldReturnOkWhenReceivedInstanceCreatedEvent() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(event.put("eventType", "DI_INVENTORY_INSTANCE_CREATED").encode())
      .post(HANDLERS_INSTANCE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
  }

  @Test
  public void shouldReturnOkWhenReceivedInstanceUpdatedEvent() throws IOException {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(ZIPArchiver.zip(event.put("eventType", "DI_INVENTORY_INSTANCE_UPDATED").encode()))
      .post(HANDLERS_INSTANCE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
  }

  @Test
  public void shouldReturnNoContentWhenReceivedRecordUpdatedEvent() {
    RestAssured.given()
      .spec(spec)
      .when()
      .body(event.put("eventType", "QM_MARC_BIB_RECORD_UPDATED").encode())
      .post(HANDLERS_UPDATED_RECORD_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }
}
