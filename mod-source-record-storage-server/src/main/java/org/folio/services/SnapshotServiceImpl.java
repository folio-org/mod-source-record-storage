package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.SnapshotDao;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.Optional;

@Component
public class SnapshotServiceImpl implements SnapshotService {

  @Autowired
  private SnapshotDao snapshotDao;

  @Override
  public Future<SnapshotCollection> getSnapshots(String query, int offset, int limit, String tenantId) {
    return snapshotDao.getSnapshots(query, offset, limit, tenantId);
  }

  @Override
  public Future<Optional<Snapshot>> getSnapshotById(String id, String tenantId) {
    return snapshotDao.getSnapshotById(id, tenantId);
  }

  @Override
  public Future<String> saveSnapshot(Snapshot snapshot, String tenantId) {
    return snapshotDao.saveSnapshot(snapshot, tenantId);
  }

  @Override
  public Future<Boolean> updateSnapshot(Snapshot snapshot, String tenantId) {
    return getSnapshotById(snapshot.getJobExecutionId(), tenantId)
      .compose(optionalSnapshot -> optionalSnapshot
        .map(t -> snapshotDao.updateSnapshot(snapshot, tenantId))
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId()))))
      );
  }

  @Override
  public Future<Boolean> deleteSnapshot(String id, String tenantId) {
    return snapshotDao.deleteSnapshot(id, tenantId);
  }
}
