package org.folio.dao;

import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;

@Component
public class PostgresClientFactory {

  private Vertx vertx;

  public PostgresClientFactory(@Autowired Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Creates instance of {@link PostgresClient}
   *
   * @param tenantId tenant id
   * @return Postgres Client
   */
  public PostgresClient createInstance(String tenantId) {
    return PostgresClient.getInstance(vertx, tenantId);
  }

}
