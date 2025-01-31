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

  @Value("${srs.marcIndexers.delete.time:01:00}")
  private String deleteTimes;

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
    LOGGER.info("deleteOldMarcIndexerVersions:: schedule time: {}", deleteTimes);
    try {
      scheduleTimes = Arrays.stream(deleteTimes.split(","))
        .map(String::trim)
        .map(time -> {
          try {
            return LocalTime.parse(time);
          } catch (DateTimeParseException e) {
            LOGGER.error("Error parsing time: '{}'. Defaulting to 01:00", time, e);
            return LocalTime.of(1, 0);
          }
        })
        .sorted()
        .collect(Collectors.toList());
      LOGGER.info("Scheduled times: {}", scheduleTimes);
    } catch (Exception e) {
      LOGGER.error("Unexpected error occurred while setting up scheduled times, defaulting to 01:00", e);
      scheduleTimes = Arrays.asList(LocalTime.of(1, 0));
    }
    scheduleNextTask(vertx, this::deleteTask);
    startFuture.complete();
  }

  private void scheduleNextTask(Vertx vertx, Runnable task) {
    long delay = calculateDelayToNextTask();
    vertx.setTimer(delay, id -> {
      task.run();
      scheduleNextTask(vertx, task);
    });
  }

  private void deleteTask() {
    if (currentDeletion.isComplete()) {
      currentDeletion = deleteOldMarcIndexerVersions(oneTimeLimit);
    } else {
      LOGGER.info("Previous marc_indexers old version deletion still ongoing");
    }
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

