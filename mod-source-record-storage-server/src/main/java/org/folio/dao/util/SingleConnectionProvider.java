package org.folio.dao.util;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.folio.rest.persist.PostgresClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;

/**
 * Util class to obtain single connection to underlying database using DriverManager.
 * Connection pool is not provided.
 */
public class SingleConnectionProvider {
  private static final String JDBC_DRIVER = "jdbc:postgresql";
  private static final String CONFIG_USERNAME_KEY = "username";
  private static final String CONFIG_PASS_KEY = "password";
  private static final String CONFIG_HOST_KEY = "host";
  private static final String CONFIG_PORT_KEY = "port";
  private static final String CONFIG_DATABASE_KEY = "database";

  private SingleConnectionProvider() {}

  /**
   * Returns database connection for the given tenant
   *
   * @param vertx  vertx instance
   * @param tenant given tenant to which schema connection should be provided
   * @return Connection to database
   * @throws SQLException if error occurs while trying to obtain access to database
   */
  public static Connection getConnection(Vertx vertx, String tenant) throws SQLException {
    JsonObject connectionConfig = PostgresClient.getInstance(vertx, tenant).getConnectionConfig();
    return getConnectionInternal(connectionConfig);
  }

  /**
   * Returns database connection for the current user
   *
   * @param vertx vertx instance
   * @return Connection to database
   * @throws SQLException if error occurs while trying to obtain access to database
   */
  public static Connection getConnection(Vertx vertx) throws SQLException {
    JsonObject connectionConfig = PostgresClient.getInstance(vertx).getConnectionConfig();
    return getConnectionInternal(connectionConfig);
  }

  private static Connection getConnectionInternal(JsonObject connectionConfig) throws SQLException {
    String username = connectionConfig.getString(CONFIG_USERNAME_KEY);
    String password = connectionConfig.getString(CONFIG_PASS_KEY);
    String host = connectionConfig.getString(CONFIG_HOST_KEY);
    String port = String.valueOf(connectionConfig.getInteger(CONFIG_PORT_KEY));
    String database = connectionConfig.getString(CONFIG_DATABASE_KEY);
    String connectionUrl = format("%s://%s:%s/%s", JDBC_DRIVER, host, port, database);
    return DriverManager.getConnection(connectionUrl, username, password);
  }
}
