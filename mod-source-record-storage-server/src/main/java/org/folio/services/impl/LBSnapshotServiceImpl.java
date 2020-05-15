package org.folio.services.impl;

import static java.lang.String.format;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.NotFoundException;

import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.LBSnapshotService;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;

@Service
public class LBSnapshotServiceImpl extends AbstractEntityService<Snapshot, SnapshotCollection, SnapshotQuery, LBSnapshotDao>
    implements LBSnapshotService {

  public Future<Snapshot> save(Snapshot snapshot, String tenantId) {
    return dao.save(setProcessingStartedDate(snapshot), tenantId);
  }

  public Future<List<Snapshot>> save(List<Snapshot> snapshots, String tenantId) {
    return dao.save(snapshots.stream().map(this::setProcessingStartedDate).collect(Collectors.toList()), tenantId);
  }

  public Future<Snapshot> update(Snapshot snapshot, String tenantId) {
    return getById(snapshot.getJobExecutionId(), tenantId)
      .compose(optionalSnapshot -> optionalSnapshot
        .map(s -> dao.update(setProcessingStartedDate(snapshot), tenantId))
        .orElse(Future.failedFuture(new NotFoundException(
          format("Snapshot with id '%s' was not found", snapshot.getJobExecutionId()))))
      );
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

}