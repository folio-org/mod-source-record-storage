package org.folio.dao;

import org.folio.dao.filter.SnapshotFilter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;

/**
 * Data access object for {@link Snapshot}
 */
public interface LBSnapshotDao extends EntityDao<Snapshot, SnapshotCollection, SnapshotFilter> {

}