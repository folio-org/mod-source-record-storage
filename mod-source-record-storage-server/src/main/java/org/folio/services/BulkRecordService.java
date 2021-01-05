package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.util.OkapiConnectionParams;
import org.jooq.Cursor;

public interface BulkRecordService {

  Future<Cursor> searchRecords(OkapiConnectionParams params);
}
