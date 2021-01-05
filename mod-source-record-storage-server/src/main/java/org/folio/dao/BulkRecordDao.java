package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import org.jooq.Condition;
import org.jooq.Cursor;

public interface BulkRecordDao {

  Future<Cursor> searchRecords(Condition condition, String tenantId);

  Future<RowStream<Row>> dummySearchRecords(String sqlQuery, Tuple bindParams);
}
