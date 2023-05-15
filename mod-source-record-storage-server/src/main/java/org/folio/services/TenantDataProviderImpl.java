package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class TenantDataProviderImpl implements TenantDataProvider {
  private Vertx vertx;

  @Autowired
  public TenantDataProviderImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<List<String>> getModuleTenants() {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    Promise<RowSet<Row>> promise = Promise.promise();
    String tenantQuery = "select nspname from pg_catalog.pg_namespace where nspname LIKE '%_mod_source_record_storage';";
    pgClient.select(tenantQuery, promise);
    return promise.future()
      .map(rowSet -> StreamSupport.stream(rowSet.spliterator(), false)
        .map(this::mapToTenant)
        .collect(Collectors.toList())
      );
  }

  private String mapToTenant(Row row) {
    String nsTenant = row.getString("nspname");
    String suffix = "_mod_source_record_storage";
    int tenantNameLength = nsTenant.length() - suffix.length();
    return nsTenant.substring(0, tenantNameLength);
  }
}
