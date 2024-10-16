package org.folio.rest.persist;

import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.folio.dao.PostgresClientFactory;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PgConnectOptionsHelper {

  public static PgConnectOptions createPgConnectOptions(JsonObject sqlConfig) {
    return PostgresClientInitializer.createPgConnectOptions(sqlConfig,
      PostgresClientFactory.HOST,
      PostgresClientFactory.PORT);
  }
}
