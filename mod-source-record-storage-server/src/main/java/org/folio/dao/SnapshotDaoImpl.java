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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.Optional;

import static java.lang.String.format;
import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

@Component
@ConditionalOnProperty(prefix = "jooq", name = "dao.snapshot", matchIfMissing = true)
public class SnapshotDaoImpl implements SnapshotDao {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDaoImpl.class);

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
  public Future<Snapshot> saveSnapshot(Snapshot snapshot, String tenantId) {
    Future<Snapshot> future = Future.future();
    pgClientFactory.createInstance(tenantId).save(SNAPSHOTS_TABLE, snapshot.getJobExecutionId(), snapshot, save -> {
      if (save.failed()) {
        LOG.error("Failed to create snapshot {}", save.cause(), snapshot.getJobExecutionId());
        future.fail(save.cause());
        return;
      }
      snapshot.setJobExecutionId(save.result());
      future.complete(snapshot);
    });
    return future;
  }

  @Override
  public Future<Snapshot> updateSnapshot(Snapshot snapshot, String tenantId) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(SNAPSHOT_ID_FIELD, snapshot.getJobExecutionId());
      pgClientFactory.createInstance(tenantId).update(SNAPSHOTS_TABLE, snapshot, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      LOG.error("Failed to update snapshot {}", e, snapshot.getJobExecutionId());
      future.fail(e);
    }
    return future
      .compose(updateResult ->
        updateResult.getUpdated() == 1 ? Future.succeededFuture(snapshot) :
          Future.failedFuture(new NotFoundException(format("Snapshot %s was not updated", snapshot.getJobExecutionId()))));
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    Future<UpdateResult> future = Future.future();
    pgClientFactory.createInstance(tenantId).delete(SNAPSHOTS_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

}
