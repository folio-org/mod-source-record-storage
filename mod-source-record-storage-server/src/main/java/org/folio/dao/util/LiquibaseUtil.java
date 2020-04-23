package org.folio.dao.util;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.folio.rest.persist.PostgresClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.folio.dao.util.SingleConnectionProvider.getConnection;

/**
 * Util class to manage liquibase scripting
 */
public class LiquibaseUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(LiquibaseUtil.class);
  private static final String CHANGELOG_MODULE_PATH = "liquibase/module/changelog.xml";
  private static final String CHANGELOG_TENANT_PATH = "liquibase/tenant/changelog.xml";
  private static final String MODULE_CONFIGURATION_SCHEMA = "source_record_storage_config";

  private LiquibaseUtil() {
  }

  /**
   * Performs initialization for module configuration schema: - creates schema for
   * module configuration - runs scripts to fill module schema
   *
   * @param vertx vertx instance
   */
  public static void initializeSchemaForModule(Vertx vertx) {
    LOGGER.info(format("Initializing schema %s for the module", MODULE_CONFIGURATION_SCHEMA));
    try (Connection connection = getConnection(vertx)) {
      createModuleConfigurationSchema(connection);
      runScripts(MODULE_CONFIGURATION_SCHEMA, connection, CHANGELOG_MODULE_PATH);
      LOGGER.info("Schema is initialized for the module");
    } catch (Exception e) {
      LOGGER.error("Error while initializing schema for the module", e);
    }
  }

  /**
   * Performs initialization for tenant schema: - runs scripts to fill tenant
   * schema in
   *
   * @param vertx  vertx instance
   * @param tenant given tenant for which database schema has to be initialized
   */
  public static void initializeSchemaForTenant(Vertx vertx, String tenant) {
    String schemaName = PostgresClient.convertToPsqlStandard(tenant);
    LOGGER.info(format("Initializing schema %s for tenant %s", schemaName, tenant));
    try (Connection connection = getConnection(vertx, tenant)) {
      runScripts(schemaName, connection, CHANGELOG_TENANT_PATH);
      LOGGER.info("Schema is initialized for tenant " + tenant);
    } catch (Exception e) {
      LOGGER.error(format("Error while initializing schema %s for tenant %s", schemaName, tenant), e);
    }
  }

  /**
   * Runs scripts for given change log in a scope of given schema
   *
   * @param schemaName    schema name
   * @param connection    connection to the underlying database
   * @param changelogPath path to changelog file in the module classpath
   * @throws LiquibaseException if database access error occurs
   */
  private static void runScripts(String schemaName, Connection connection, String changelogPath)
      throws LiquibaseException {
    Liquibase liquibase = null;
    try {
      Database database = DatabaseFactory.getInstance()
          .findCorrectDatabaseImplementation(new JdbcConnection(connection));
      database.setDefaultSchemaName(schemaName);
      liquibase = new Liquibase(changelogPath, new ClassLoaderResourceAccessor(), database);
      liquibase.update(new Contexts());
    } finally {
      if (liquibase != null && liquibase.getDatabase() != null) {
        Database database = liquibase.getDatabase();
        database.close();
      }
    }
  }

  /**
   * Creates module configuration schema
   *
   * @param connection connection to the underlying database
   * @throws SQLException if query execution error occurs
   */
  private static void createModuleConfigurationSchema(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String sql = format("create schema if not exists %s", MODULE_CONFIGURATION_SCHEMA);
      statement.execute(sql);
    }
  }
}
