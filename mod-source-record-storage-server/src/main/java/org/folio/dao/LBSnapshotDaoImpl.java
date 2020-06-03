package org.folio.dao;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.folio.dao.util.LBSnapshotDaoUtil;
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
public class LBSnapshotDaoImpl implements LBSnapshotDao {

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public LBSnapshotDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    return getQueryExecutor(tenantId).transaction(txQE -> {
      SnapshotCollection snapshotCollection = new SnapshotCollection();
      return CompositeFuture.all(
        LBSnapshotDaoUtil.findByCondition(txQE, condition, orderFields, offset, limit)
          .map(snapshots -> addSnapshots(snapshotCollection, snapshots)),
        LBSnapshotDaoUtil.countByCondition(txQE, condition)
          .map(totalRecords -> addTotalRecords(snapshotCollection,totalRecords))
      ).map(res -> snapshotCollection);
    });
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    return LBSnapshotDaoUtil.findById(getQueryExecutor(tenantId), id);
  }

  @Override
  public Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId) {
    return LBSnapshotDaoUtil.save(getQueryExecutor(tenantId), snapshot);
  }

  @Override
  public Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId) {
    return LBSnapshotDaoUtil.update(getQueryExecutor(tenantId), snapshot);
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    return LBSnapshotDaoUtil.delete(getQueryExecutor(tenantId), id);
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