package org.folio.services;

import static java.lang.String.format;
import static org.folio.dao.PostgresClientFactory.configuration;
import static org.folio.dao.util.MappingUtil.snapshotsFromDatabase;
import static org.folio.dao.util.MappingUtil.toDatabase;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.dao.PostgresClientFactory;
import org.folio.dao.util.MappingUtil;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.jooq.tables.daos.SnapshotsLbDao;
import org.folio.rest.jooq.tables.pojos.SnapshotsLb;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;

@Service
@ConditionalOnProperty(prefix = "liquibase", name = "services.snapshot", havingValue = "true")
public class LBSnapshotServiceImpl implements LBSnapshotService {

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public LBSnapshotServiceImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields, int offset, int limit, String tenantId) {
    SnapshotsLbDao snapshotsLbDao = getSnapshotDao(tenantId);
    return snapshotsLbDao.queryExecutor()
      .findMany(dslContext -> dslContext.selectFrom(snapshotsLbDao.getTable())
        .where(condition)
        .orderBy(orderFields)
        .offset(offset)
        .limit(limit))
      .map(this::toCollection);
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    return getSnapshotDao(tenantId)
      .findOneById(UUID.fromString(id))
      .map(MappingUtil::fromDatabase)
      .map(Optional::ofNullable);
  }

  @Override
  public Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId) {
    return getSnapshotDao(tenantId)
      .insert(toDatabase(snapshot))
      .map(i -> snapshot);
  }

  @Override
  public Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId) {
    SnapshotsLbDao snapshotsLbDao = getSnapshotDao(tenantId);
    String id = snapshot.getJobExecutionId();
    return snapshotsLbDao.findOneById(UUID.fromString(id))
      .map(Optional::ofNullable)
      .compose(optionalSnapshot -> optionalSnapshot
        .map(os -> setProcessingStartedDate(snapshot))
        .map(s -> snapshotsLbDao.update(toDatabase(s)).map(i -> s))
      .orElse(Future.failedFuture(getNotFoundException(id)))
    );
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    return getSnapshotDao(tenantId)
      .deleteById(UUID.fromString(id))
      .map(d -> d > 0);
  }

  @Override
  public SnapshotsLbDao getSnapshotDao(String tenantId) {
    return new SnapshotsLbDao(configuration, postgresClientFactory.getClient(tenantId));
  }

  private SnapshotCollection toCollection(List<SnapshotsLb> snapshots) {
    return new SnapshotCollection()
      .withSnapshots(snapshotsFromDatabase(snapshots))
      .withTotalRecords(snapshots.size());
  }

  /**
   * Sets processing start date if snapshot status is PARSING_IN_PROGRESS
   *
   * @param snapshot snapshot
   * @return snapshot with populated processingStartedDate field if snapshot status is PARSING_IN_PROGRESS
   */
  private Snapshot setProcessingStartedDate(Snapshot snapshot) {
    if (Snapshot.Status.PARSING_IN_PROGRESS.equals(snapshot.getStatus())) {
      snapshot.setProcessingStartedDate(new Date());
    }
    return snapshot;
  }

  private NotFoundException getNotFoundException(String id) {
    return new NotFoundException(format("Snapshot with id '%s' was not found", id));
  }

}