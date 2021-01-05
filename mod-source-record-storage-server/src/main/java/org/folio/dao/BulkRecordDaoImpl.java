package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
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
    return Future.succeededFuture();
  }

  public Future<RowStream<Row>> dummySearchRecords(String sqlQuery, Tuple bindParams) {
    PgPool client = PostgresClientFactory.getCachedPool();

    Promise<RowStream<Row>> promise = Promise.promise();
    client.getConnection(car -> {
      SqlConnection connection = car.result();
      connection.prepare(sqlQuery, psar -> {
        /*
         * this read stream should be wrapped into another read stream
         * and the purpose of that wrapper is to close connection
         * before doing that check if it is really needed.
         */
        RowStream<Row> rowStream = psar.result().createStream(100, bindParams);
        promise.complete(rowStream);
      });
    });
    return promise.future();
  }
}