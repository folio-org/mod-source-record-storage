package org.folio.services;

import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;

/**
 * {@link Snapshot} service
 */
public interface LBSnapshotService extends EntityService<Snapshot, SnapshotCollection, SnapshotQuery> {

}