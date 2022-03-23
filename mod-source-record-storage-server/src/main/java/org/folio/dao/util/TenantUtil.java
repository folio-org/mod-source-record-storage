package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.persist.PostgresClient;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/* The util class helps to work with tenant's data */
public class TenantUtil {

  /* The method returns all the available tenants for whom the mode is deployed */
  public static Future<List<String>> getModuleTenants(Vertx vertx) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    Promise<RowSet<Row>> promise = Promise.promise();
    String tenantQuery = "select nspname from pg_catalog.pg_namespace where nspname LIKE '%_mod_source_record_storage';";
    pgClient.select(tenantQuery, promise);
    return promise.future()
      .map(rowSet -> StreamSupport.stream(rowSet.spliterator(), false)
        .map(TenantUtil::mapToTenant)
        .collect(Collectors.toList())
      );
  }

  private static String mapToTenant(Row row) {
    String nsTenant = row.getString("nspname");
    String suffix = "_mod_source_record_storage";
    int suffixLength = nsTenant.length() - suffix.length();
    return nsTenant.substring(0, suffixLength);
  }
}
