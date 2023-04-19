package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.RecordDao;
import org.folio.services.TenantDataProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

@Component
@Scope(SCOPE_PROTOTYPE)
public class MarcIndexersVersionDeletionVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger();

  @Autowired
  private RecordDao recordDao;

  @Autowired
  private TenantDataProvider tenantDataProvider;

  @Value("${srs.marcIndexers.delete.interval.seconds:1800}")
  private int interval;

  private Future<Boolean> currentDeletion = Future.succeededFuture();

  @Override
  public void start(Promise<Void> startFuture) {
    vertx.setPeriodic(interval * 1000L, id -> {
      if (currentDeletion.isComplete()) {
        currentDeletion = deleteOldMarcIndexerVersions();
      } else {
        LOGGER.info("Previous marc_indexers old version deletion still ongoing");
      }
    });
    startFuture.complete();
  }

  private Future<Boolean> deleteOldMarcIndexerVersions() {
    LOGGER.info("Performing marc_indexers old versions deletion...");
    long startTime = System.nanoTime();
    return tenantDataProvider.getModuleTenants()
      .onFailure(ar ->
        LOGGER.error("could not get the list of tenants to delete marc indexer versions", ar.getCause()))
      .compose(ar -> {
        Future<Boolean> future = Future.succeededFuture();
        if (ar.size() == 0) {
          LOGGER.info("no tenants available for marc_indexers deletion");
        }
        for (String tenantId : ar) {
          future = future.compose(Void -> recordDao.deleteMarcIndexersOldVersions(tenantId));
        }
        return future;
      })
      .onSuccess(ar -> {
        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000.0 / 1_000;
        LOGGER.info("marc_indexers old versions deletion completed. duration={}s", durationSeconds);
      });
  }
}

