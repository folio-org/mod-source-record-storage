package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.RecordDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordCleanupServiceImpl implements RecordCleanupService {
  private static final Logger LOGGER = LogManager.getLogger();
  private final RecordDao recordDao;
  /* The default delay in milliseconds equals 24 hours */
  private long cleanupDelay = 24 * 3600_000;

  @Autowired
  public RecordCleanupServiceImpl(RecordDao recordDao) {
    this.recordDao = recordDao;
  }

  public RecordCleanupServiceImpl(RecordDao recordDao, long cleanupDelay) {
    this.recordDao = recordDao;
    this.cleanupDelay = cleanupDelay;
  }

  @Override
  public long initialize(Vertx vertx, String tenantId) {
    return vertx.setPeriodic(cleanupDelay, periodicHandler ->
      vertx.<Void>executeBlocking(blockingHandler -> cleanup(tenantId)
        .onFailure(throwable -> LOGGER.error("Error while cleaning up records, cause: ", throwable))
        .onSuccess(ar -> LOGGER.info("Records have been successfully purged"))
    ));
  }

  /* Performs clean & purge a records */
  private Future<Void> cleanup(String tenantId) {
    return recordDao.cleanRecords(tenantId);
  }
}
