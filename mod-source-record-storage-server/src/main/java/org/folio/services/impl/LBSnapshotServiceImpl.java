package org.folio.services.impl;

import org.folio.dao.LBSnapshotDao;
import org.folio.dao.query.SnapshotQuery;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SnapshotCollection;
import org.folio.services.AbstractEntityService;
import org.folio.services.LBSnapshotService;
import org.springframework.stereotype.Service;

@Service
public class LBSnapshotServiceImpl extends AbstractEntityService<Snapshot, SnapshotCollection, SnapshotQuery, LBSnapshotDao>
    implements LBSnapshotService {

}