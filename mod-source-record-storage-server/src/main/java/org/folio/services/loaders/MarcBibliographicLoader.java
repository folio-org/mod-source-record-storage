package org.folio.services.loaders;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.folio.DataImportEventPayload;
import org.folio.dao.RecordDaoImpl;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;


public class MarcBibliographicLoader implements MatchValueLoader {

  private static final Logger LOG = LoggerFactory.getLogger(MarcBibliographicLoader.class);

  @Autowired
  private Vertx vertx;

  @Override
  public CompletableFuture<LoadResult> loadEntity(LoadQuery loadQuery, DataImportEventPayload dataImportEventPayload) {
    if (loadQuery == null) {
      return CompletableFuture.completedFuture(new LoadResult());
    }
    CompletableFuture<LoadResult> future = new CompletableFuture<>();
    LoadResult loadResult = new LoadResult();
    loadResult.setEntityType(String.valueOf(EntityType.MARC_BIBLIOGRAPHIC));
    HashMap<String, String> context = dataImportEventPayload.getContext();

    PostgresClient postgresClient = PostgresClient.getInstance(vertx, context.get("tenantId"));
    postgresClient.execute(loadQuery.getSql(), ar -> {
      if (ar.succeeded()) {
        RowSet<Row> result = ar.result();
        if (result.size() == 1) {
          for (Row row : result) {
            row.getString()
          }
          loadResult.setValue(result.toString()); // Map to record.json
        } else if (result.size() > 1) {
          String errorMessage = "Found multiple records matching specified conditions";
          LOG.error(errorMessage);
          future.completeExceptionally(new MatchingException(errorMessage));
        }
        future.complete(loadResult);
      } else {
        LOG.error(ar.cause());
        future.completeExceptionally(new MatchingException(ar.cause()));
      }
    });
    return future;
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return entityType == EntityType.MARC_BIBLIOGRAPHIC;
  }
}
