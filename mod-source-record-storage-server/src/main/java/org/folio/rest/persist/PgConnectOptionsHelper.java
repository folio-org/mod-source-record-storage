package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PgConnectOptionsHelper {

  public static PgConnectOptions createPgConnectOptions(JsonObject sqlConfig) {
    return PostgresClient.createPgConnectOptions(sqlConfig, false);
  }
}
