package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.persist.PostgresClient;

import java.util.List;
import java.util.stream.StreamSupport;

/**
 * The util class helps to work with tenant's data
 */
public class TenantUtil {

  private TenantUtil() {
  }

  /* The method returns all the available tenants for whom the mode is deployed */
  public static Future<List<String>> getModuleTenants(Vertx vertx) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    Promise<RowSet<Row>> promise = Promise.promise();
    String tenantQuery = "select nspname from pg_catalog.pg_namespace where nspname LIKE '%_mod_source_record_storage';";
    pgClient.selectRead(tenantQuery, 60000, promise::handle);
    return promise.future()
      .map(rowSet -> StreamSupport.stream(rowSet.spliterator(), false)
        .map(TenantUtil::mapToTenant)
        .toList());
  }

  private static String mapToTenant(Row row) {
    String nsTenant = row.getString("nspname");
    String suffix = "_mod_source_record_storage";
    int tenantNameLength = nsTenant.length() - suffix.length();
    return nsTenant.substring(0, tenantNameLength);
  }

  public static double calculateDurationSeconds(long startTime) {
    long endTime =  System.nanoTime();
    return (endTime - startTime) / 1_000_000.0 / 1_000;
  }
}
