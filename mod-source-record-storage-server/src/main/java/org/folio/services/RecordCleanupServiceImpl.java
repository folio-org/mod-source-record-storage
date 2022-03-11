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
  private long cleanupDelay = 24L * 3600_000L;

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
    return vertx.setPeriodic(cleanupDelay, periodicHandler -> cleanup(tenantId)
      .onFailure(throwable -> LOGGER.error("An error occurred during cleanup process, cause: ", throwable))
      .onSuccess(ar -> LOGGER.info("Cleanup process has been successfully completed"))
    );
  }

  /* Performs clean & purge a records */
  private Future<Void> cleanup(String tenantId) {
    return recordDao.cleanRecords(tenantId);
  }
}
