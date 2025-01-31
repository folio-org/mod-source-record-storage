package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.RecordDao;
import org.folio.services.TenantDataProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

/**
 * This verticle is responsible for periodically deleting old versions of Marc Indexers for all tenants.
 * The interval between each deletion can be configured using the "srs.marcIndexers.delete.interval.seconds"
 * system property.
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class MarcIndexersVersionDeletionVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final ZoneId ZONE_ID = ZoneId.systemDefault();

  private final RecordDao recordDao;
  private final TenantDataProvider tenantDataProvider;
  private List<LocalTime> scheduleTimes;

  @Value("${srs.marcIndexers.delete.interval.seconds:1800}")
  private int interval;

  @Value("${srs.marcIndexers.delete.plannedTime:00:00}")
  private String plannedTime;

  @Value("${srs.marcIndexers.delete.oneTimeLimit:100000}")
  private Integer oneTimeLimit;

  private Future<Boolean> currentDeletion = Future.succeededFuture();

  @Autowired
  public MarcIndexersVersionDeletionVerticle(RecordDao recordDao, TenantDataProvider tenantDataProvider) {
    this.recordDao = recordDao;
    this.tenantDataProvider = tenantDataProvider;
  }

  @Override
  public void start(Promise<Void> startFuture) {
    long intervalMillis = interval * 1000L;

    if (!"00:00".equals(plannedTime) && interval == 1800) {
      LOGGER.info("Using scheduler based on planned time: {}", plannedTime);
      setupTimedDeletion(plannedTime);
    } else {
      LOGGER.info("Using periodic interval scheduler: {}s", interval);
      setupPeriodicDeletion(intervalMillis);
    }

    startFuture.complete();
  }

  private void setupPeriodicDeletion(long intervalMillis) {
    LOGGER.info("Setting up periodic deletion every {}s", interval);
    vertx.setPeriodic(intervalMillis, id -> executeDeletionTask());
  }

  private void setupTimedDeletion(String plannedTime) {
    LOGGER.info("Setting up timed deletion based on planned times: {}", plannedTime);
    try {
      scheduleTimes = Arrays.stream(plannedTime.split(","))
        .map(String::trim)
        .map(LocalTime::parse)
        .sorted()
        .collect(Collectors.toList());
      LOGGER.info("Scheduled times for deletion: {}", scheduleTimes);
      scheduleNextTask(vertx, this::executeDeletionTask);
    } catch (DateTimeParseException e) {
      LOGGER.error("Error parsing time, defaulting to periodic deletion with default interval", e);
      setupPeriodicDeletion(1800 * 1000L);
    }
  }

  private void executeDeletionTask() {
    if (currentDeletion.isComplete()) {
      currentDeletion = deleteOldMarcIndexerVersions(oneTimeLimit);
    } else {
      LOGGER.info("Previous marc_indexers old version deletion still ongoing.");
    }
  }

  private void scheduleNextTask(Vertx vertx, Runnable task) {
    long delay = calculateDelayToNextTask();
    LOGGER.info("Scheduling next task with delay: {}s", delay/1000L);
    vertx.setTimer(delay, id -> {
      task.run();
      scheduleNextTask(vertx, task);
    });
  }

  private long calculateDelayToNextTask() {
    ZonedDateTime now = ZonedDateTime.now(ZONE_ID);
    ZonedDateTime nextRun = now.with(scheduleTimes.get(0));
    for (LocalTime time : scheduleTimes) {
      ZonedDateTime potentialNextRun = now.with(time);
      if (now.compareTo(potentialNextRun) <= 0) {
        nextRun = potentialNextRun;
        break;
      }
    }
    if (now.compareTo(nextRun) > 0) {
      nextRun = nextRun.plusDays(1).with(scheduleTimes.get(0));
    }
    return ChronoUnit.MILLIS.between(now, nextRun);
  }

  /**
   * Deletes old versions of Marc Indexers for all tenants in the system and returns a Future of Boolean.
   */
  Future<Boolean> deleteOldMarcIndexerVersions(Integer oneTimeLimit) {
    LOGGER.info("Performing marc_indexers old versions deletion...");
    long startTime = System.nanoTime();
    return tenantDataProvider.getModuleTenants("marc_records_tracking")
      .onFailure(ar ->
        LOGGER.error("could not get the list of tenants to delete marc indexer versions", ar.getCause()))
      .compose(ar -> {
        Future<Boolean> future = Future.succeededFuture();
        if (ar.isEmpty()) {
          LOGGER.info("no tenants available for marc_indexers deletion");
        }
        for (String tenantId : ar) {
          future = future.compose(v -> recordDao.deleteMarcIndexersOldVersions(tenantId, oneTimeLimit));
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

