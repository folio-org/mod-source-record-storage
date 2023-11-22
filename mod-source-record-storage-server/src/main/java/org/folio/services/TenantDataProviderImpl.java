package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class TenantDataProviderImpl implements TenantDataProvider {
  private static final String SUFFIX = "_mod_source_record_storage";
  private Vertx vertx;

  @Autowired
  public TenantDataProviderImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<List<String>> getModuleTenants(String table) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    String tenantQuery = """
                    select schemaname from pg_catalog.pg_tables
                    where schemaname LIKE $1 and tablename = $2
                    """;
    return pgClient.execute(tenantQuery, Tuple.of("%" + SUFFIX, table))
        .map(rowSet -> StreamSupport.stream(rowSet.spliterator(), false)
            .map(this::mapToTenant)
            .collect(Collectors.toList()));
  }

  private String mapToTenant(Row row) {
    String schemaname = row.getString("schemaname");
    int tenantNameLength = schemaname.length() - SUFFIX.length();
    return schemaname.substring(0, tenantNameLength);
  }
}
