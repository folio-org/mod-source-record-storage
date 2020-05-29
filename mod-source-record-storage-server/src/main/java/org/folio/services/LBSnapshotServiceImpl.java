package org.folio.services;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.folio.dao.LBSnapshotDao;
import org.folio.dao.PostgresClientFactory;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
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
    return LBSnapshotDao.findByCondition(getQueryExecutor(tenantId), condition, orderFields, offset, limit)
      .map(this::toCollection);
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    return LBSnapshotDao.findById(getQueryExecutor(tenantId), id);
  }

  @Override
  public Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId) {
    return LBSnapshotDao.save(getQueryExecutor(tenantId), snapshot);
  }

  @Override
  public Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId) {
    return LBSnapshotDao.update(getQueryExecutor(tenantId), snapshot);
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    return LBSnapshotDao.delete(getQueryExecutor(tenantId), id);
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private SnapshotCollection toCollection(List<Snapshot> snapshots) {
    return new SnapshotCollection()
      .withSnapshots(snapshots)
      .withTotalRecords(snapshots.size());
  }

}