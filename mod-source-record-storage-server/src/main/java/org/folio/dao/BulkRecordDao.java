package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;

public interface BulkRecordDao {

  Future<RowStream<Row>> searchRecords(String sqlQuery, Tuple bindParams, String tenantId);
}
