package org.folio.dao;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import io.vertx.reactivex.sqlclient.SqlConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.SnapshotDaoUtil;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.jooq.Condition;
import org.jooq.OrderField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;

import io.vertx.core.Future;

@Component
public class SnapshotDaoImpl implements SnapshotDao {

  private static final Logger LOG = LogManager.getLogger();
  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public SnapshotDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<SnapshotCollection> getSnapshots(Condition condition, Collection<OrderField<?>> orderFields,
      int offset, int limit, String tenantId) {
    return postgresClientFactory.getCachedPool(tenantId).withTransaction((SqlConnection sqlConnection) -> {
      SnapshotCollection snapshotCollection = new SnapshotCollection();
      return Future.all(
        SnapshotDaoUtil.findByCondition(sqlConnection, condition, orderFields, offset, limit)
          .map(snapshots -> addSnapshots(snapshotCollection, snapshots)),
        SnapshotDaoUtil.countByCondition(sqlConnection, condition)
          .map(totalRecords -> addTotalRecords(snapshotCollection, totalRecords))
      ).map(res -> snapshotCollection);
    });
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    return SnapshotDaoUtil.findById(postgresClientFactory.getCachedPool(tenantId), id);
  }

  @Override
  public Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId) {
    LOG.trace("saveSnapshot:: Saving snapshot with jobExecutionId {} for tenant {}", snapshot.getJobExecutionId(), tenantId);
    return SnapshotDaoUtil.save(postgresClientFactory.getCachedPool(tenantId), snapshot);
  }

  @Override
  public Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId) {
    LOG.trace("updateSnapshot:: Updating snapshot with jobExecutionId {} for tenant {}", snapshot.getJobExecutionId(), tenantId);
    return SnapshotDaoUtil.update(postgresClientFactory.getCachedPool(tenantId), snapshot);
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    LOG.trace("deleteSnapshot:: Deleting snapshot {} for tenant {}", id, tenantId);
    return SnapshotDaoUtil.delete(postgresClientFactory.getCachedPool(tenantId), id);
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
