package org.folio.dao;

import io.vertx.core.Future;
import org.jooq.Condition;
import org.jooq.Cursor;

public interface BulkRecordDao {

  Future<Cursor> searchRecords(Condition condition, String tenantId);
}
