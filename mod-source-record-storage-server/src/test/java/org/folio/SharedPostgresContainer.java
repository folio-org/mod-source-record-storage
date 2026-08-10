package org.folio;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.persist.PostgresClient;

/**
 * Starts a single PostgreSQL test container ({@link PostgresTesterContainer}) shared by every test
 * running in the same JVM fork.
 *
 * <p>Surefire runs the whole module in one reused fork ({@code forkCount=1}, {@code reuseForks=true}).
 * Previously every test base class started (and stopped) its own container via
 * {@link PostgresClient#setPostgresTester(org.folio.util.PostgresTester)}. Because RMB caches clients
 * and the mapped container port ends up in JVM-wide state ({@code Envs}, the RMB connection pool and
 * {@code PostgresClientFactory} caches), a class that stopped its container left later classes pointing
 * at an already stopped port, producing {@code Connection refused}. Starting the container once and
 * never stopping it (Testcontainers' Ryuk and the RMB shutdown hook reap it at JVM exit) keeps the
 * connection details stable for the whole run.
 */
public final class SharedPostgresContainer {

  private static final Vertx VERTX = Vertx.vertx();

  private static JsonObject connectionConfig;

  private SharedPostgresContainer() {
  }

  /**
   * Start the shared container on first call and return its connection configuration
   * ({@code host}, {@code port}, {@code username}, {@code password}, {@code database}).
   *
   * @return a copy of the connection configuration of the shared container
   */
  public static synchronized JsonObject getConnectionConfig() {
    if (connectionConfig == null) {
      PostgresClient.setPostgresTester(new PostgresTesterContainer());
      // getConnectionConfig() lazily starts the container and exposes its mapped host/port
      connectionConfig = PostgresClient.getInstance(VERTX).getConnectionConfig().copy();
    }
    return connectionConfig;
  }
}
