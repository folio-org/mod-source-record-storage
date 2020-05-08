package org.folio.services;

import org.folio.dao.LBSnapshotDao;
import org.folio.dao.filter.SnapshotFilter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;

/**
 * {@link Snapshot} service
 */
public interface LBSnapshotService extends EntityService<Snapshot, SnapshotCollection, SnapshotFilter, LBSnapshotDao> {

}