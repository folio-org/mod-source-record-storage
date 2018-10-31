package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.SnapshotDao;
import org.folio.dao.SnapshotDaoImpl;
import org.folio.rest.jaxrs.model.Snapshot;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Optional;

public class SnapshotServiceImpl implements SnapshotService {

  private Vertx vertx;
  private SnapshotDao snapshotDao;

  public SnapshotServiceImpl(SnapshotDao snapshotDao) {
    this.snapshotDao = snapshotDao;
  }

  public SnapshotServiceImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    snapshotDao = new SnapshotDaoImpl(vertx, tenantId);
  }

  @Override
  public Future<List<Snapshot>> getSnapshots(String query, int offset, int limit) {
    return snapshotDao.getSnapshots(query, offset, limit);
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id) {
    return snapshotDao.getSnapshotById(id);
  }

  @Override
  public Future<String> saveSnapshot(Snapshot snapshot) {
    return snapshotDao.saveSnapshot(snapshot);
  }

  @Override
  public Future<Boolean> updateSnapshot(Snapshot snapshot) {
    return getSnapshotById(snapshot.getJobExecutionId())
      .compose(optionalSnapshot -> optionalSnapshot
        .map(t -> snapshotDao.updateSnapshot(snapshot))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId()))))
      );
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id) {
    return snapshotDao.deleteSnapshot(id);
  }
}
