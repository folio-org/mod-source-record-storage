package org.folio.dao;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

@Component
public class SnapshotDaoImpl implements SnapshotDao {

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public SnapshotDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      SnapshotCollection snapshotCollection = new SnapshotCollection();
      return CompositeFuture.all(
        SnapshotDaoUtil.findByCondition(txQE, condition, orderFields, offset, limit)
          .map(snapshots -> addSnapshots(snapshotCollection, snapshots)),
        SnapshotDaoUtil.countByCondition(txQE, condition)
          .map(totalRecords -> addTotalRecords(snapshotCollection,totalRecords))
      ).map(res -> snapshotCollection);
    });
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    return SnapshotDaoUtil.findById(getQueryExecutor(tenantId), id);
  }

  @Override
  public Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId) {
    return SnapshotDaoUtil.save(getQueryExecutor(tenantId), snapshot);
  }

  @Override
  public Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId) {
    return SnapshotDaoUtil.update(getQueryExecutor(tenantId), snapshot);
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    return SnapshotDaoUtil.delete(getQueryExecutor(tenantId), id);
  }

  private ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return postgresClientFactory.getQueryExecutor(tenantId);
  }

  private SnapshotCollection addSnapshots(SnapshotCollection snapshotCollection, List<Snapshot> snapshots) {
    return snapshotCollection.withSnapshots(snapshots);
  }

  private SnapshotCollection addTotalRecords(SnapshotCollection snapshotCollection, Integer totalRecords) {
    return snapshotCollection.withTotalRecords(totalRecords);
  }

}