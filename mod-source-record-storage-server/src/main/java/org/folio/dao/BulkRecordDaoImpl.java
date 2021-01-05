package org.folio.dao;

import io.vertx.core.Future;
import org.folio.dao.util.RecordDaoUtil;
import org.folio.rest.jaxrs.model.Record;
import org.jooq.Condition;

import org.jooq.Cursor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BulkRecordDaoImpl implements BulkRecordDao {

  private final PostgresClientFactory postgresClientFactory;

  @Autowired
  public BulkRecordDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Cursor> searchRecords(Condition condition, String tenantId) {
//    return postgresClientFactory.getQueryExecutor(tenantId).query(
//      dsl -> dsl.selectFrom(RECORDS_LB)
//      .where(condition)
//      .fetchLazy();
//    );
    return Future.succeededFuture();
  }
}
