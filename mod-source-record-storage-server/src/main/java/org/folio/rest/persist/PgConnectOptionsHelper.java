package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PgConnectOptionsHelper {

  public static PgConnectOptions createPgConnectOptions(JsonObject sqlConfig) {
    return PostgresClientInitializer.createPgConnectOptions(sqlConfig, PostgresClient.HOST, PostgresClient.PORT);
  }
}
