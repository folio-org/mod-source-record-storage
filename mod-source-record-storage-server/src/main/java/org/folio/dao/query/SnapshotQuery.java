package org.folio.dao.query;

import static org.folio.dao.impl.LBSnapshotDaoImpl.PROCESSING_STARTED_DATE_COLUMN_NAME;
import static org.folio.dao.impl.LBSnapshotDaoImpl.STATUS_COLUMN_NAME;
import static org.folio.dao.util.DaoUtil.ID_COLUMN_NAME;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.folio.rest.jaxrs.model.Snapshot;

public class SnapshotQuery extends AbstractEntityQuery {

  private static final Map<String, String> ptc = new HashMap<>();

  static {
    ptc.put("jobExecutionId", ID_COLUMN_NAME);
    ptc.put("status", STATUS_COLUMN_NAME);
    ptc.put("processingStartedDate", PROCESSING_STARTED_DATE_COLUMN_NAME);
  }

  private SnapshotQuery() {
    super(ImmutableMap.copyOf(ptc), Snapshot.class);
  }

  public static SnapshotQuery query() {
    return new SnapshotQuery();
  }

}