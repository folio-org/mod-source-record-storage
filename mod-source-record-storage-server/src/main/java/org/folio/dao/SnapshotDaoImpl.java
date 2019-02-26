package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

@Component
public class SnapshotDaoImpl implements SnapshotDao {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDaoImpl.class);

  private static final String SNAPSHOTS_TABLE = "snapshots";
  private static final String SNAPSHOT_ID_FIELD = "'jobExecutionId'";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<SnapshotCollection> getSnapshots(String query, int offset, int limit, String tenantId) {
    Future<Results<Snapshot>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(SNAPSHOTS_TABLE, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(SNAPSHOTS_TABLE, Snapshot.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while querying snapshots", e);
      future.fail(e);
    }
    return future.map(results -> new SnapshotCollection()
      .withSnapshots(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    Future<Results<Snapshot>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(SNAPSHOT_ID_FIELD, id);
      pgClientFactory.createInstance(tenantId).get(SNAPSHOTS_TABLE, Snapshot.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error querying snapshots by id", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(snapshots -> snapshots.isEmpty() ? Optional.empty() : Optional.of(snapshots.get(0)));
  }

  @Override
  public Future<String> saveSnapshot(Snapshot snapshot, String tenantId) {
    Future<String> future = Future.future();
    pgClientFactory.createInstance(tenantId).save(SNAPSHOTS_TABLE, snapshot.getJobExecutionId(), snapshot, future.completer());
    return future;
  }

  @Override
  public Future<Boolean> updateSnapshot(Snapshot snapshot, String tenantId) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(SNAPSHOT_ID_FIELD, snapshot.getJobExecutionId());
      pgClientFactory.createInstance(tenantId).update(SNAPSHOTS_TABLE, snapshot, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      LOG.error("Error updating snapshots", e);
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    Future<UpdateResult> future = Future.future();
    pgClientFactory.createInstance(tenantId).delete(SNAPSHOTS_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

}
