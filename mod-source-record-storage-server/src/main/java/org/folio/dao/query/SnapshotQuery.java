package org.folio.dao.query;

import static org.folio.dao.impl.LBSnapshotDaoImpl.PROCESSING_STARTED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBSnapshotDaoImpl.STATUS_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.SNAPSHOTS_TABLE_NAME;

import org.folio.dao.query.Metamodel.Property;
import org.folio.rest.jaxrs.model.Snapshot;

@Metamodel(
  entity = Snapshot.class,
  table = SNAPSHOTS_TABLE_NAME,
  properties = {
    @Property(path = "jobExecutionId", column = ID_COLUMN_NAME),
    @Property(path = "status", column = STATUS_COLUMN_NAME),
    @Property(path = "processingStartedDate", column = PROCESSING_STARTED_DATE_COLUMN_NAME)
  }
)
public class SnapshotQuery extends AbstractEntityQuery<SnapshotQuery> {

  public static SnapshotQuery query() {
    return new SnapshotQuery();
  }

}