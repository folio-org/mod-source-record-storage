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
  /* The default number of days from the current when a records were 'deleted' */
  private int lastUpdatedDays = 7;
  /* The default number of 'deleted' records that has to be cleaned up at once */
  private int limit = 1000;

  @Autowired
  public RecordCleanupServiceImpl(RecordDao recordDao) {
    this.recordDao = recordDao;
  }

  public RecordCleanupServiceImpl(RecordDao recordDao, long cleanupDelay, int lastUpdatedDays, int limit) {
    this.recordDao = recordDao;
    this.cleanupDelay = cleanupDelay;
    this.lastUpdatedDays = lastUpdatedDays;
    this.limit = limit;
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
    return recordDao.deleteRecords(tenantId, lastUpdatedDays, limit);
  }
}
