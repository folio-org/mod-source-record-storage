package org.folio.dao;

import org.folio.dao.filter.SnapshotFilter;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;

public interface LBSnapshotDao extends BeanDao<Snapshot, SnapshotCollection, SnapshotFilter> {

}