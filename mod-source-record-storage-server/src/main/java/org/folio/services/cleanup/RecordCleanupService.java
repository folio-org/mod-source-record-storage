package org.folio.services.cleanup;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.RecordDao;
import org.folio.dao.util.TenantUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class RecordCleanupService {
  private static final Logger LOGGER = LogManager.getLogger();
  private final Vertx vertx;
  private final RecordDao recordDao;
  private final int lastUpdatedDays;
  private final int limit;

  public RecordCleanupService(@Value("${srs.cleanup.last.updated.days:7}") int lastUpdatedDays,
                              @Value("${srs.cleanup.limit:100}") int limit,
                              @Autowired Vertx vertx,
                              @Autowired RecordDao recordDao) {
    this.vertx = vertx;
    this.recordDao = recordDao;
    this.lastUpdatedDays = lastUpdatedDays;
    this.limit = limit;
  }

  /**
   * The method is getting run by the Spring Framework's Scheduler, at 12 am (midnight) every day by default.
   * The execution starts automatically on the ApplicationContext setup.
   * The schedule is defined by the cron expression, which allows defining timing in a fixed format.
   */
  @Scheduled(cron = "${srs.cleanup.cron.expression:0 0 0 * * ?}")
  public void cleanup() {
    TenantUtil.getModuleTenants(vertx)
      .onFailure(throwable -> LOGGER.error("Failed to retrieve tenants available for the module, cause: {}", throwable.getMessage()))
      .onSuccess(tenants -> {
        for (String tenantId : tenants) {
          recordDao.deleteRecords(lastUpdatedDays, limit, tenantId)
            .onFailure(throwable -> LOGGER.error("Failed to delete records, tenant: {}, cause: {}", tenantId, throwable.getMessage()))
            .onSuccess(ar -> LOGGER.info("Records has been successfully deleted, tenant: {}", tenantId));
        }
      });
  }
}

