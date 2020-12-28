package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BulkRecordDaoImpl implements BulkRecordDao {

  private Vertx vertx;

  @Autowired
  public BulkRecordDaoImpl(final Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<RowStream<Row>> searchRecords(String sqlQuery, Tuple bindParams, String tenantId) {
    PgPool pgPool = PostgresClientFactory.getCachedPool(vertx, tenantId);

    Promise<RowStream<Row>> promise = Promise.promise();
    pgPool.getConnection(connectionAsyncResult -> {
      SqlConnection connection = connectionAsyncResult.result();
      connection.prepare(sqlQuery, preparedStatementAr -> {
        if (preparedStatementAr.succeeded()) {
          PreparedStatement preparedStatement = preparedStatementAr.result();
          // Streams require to run within a transaction
          Transaction transaction = connection.begin();

          RowStream<Row> rowStream = preparedStatement.createStream(1, bindParams);

          rowStream.exceptionHandler(err -> {
            System.out.println("Error: " + err.getMessage());
          });
          rowStream.endHandler(v -> {
            transaction.commit();
            System.out.println("End of stream");
          });
          promise.complete(rowStream);
        }
      });
    });
    return promise.future();
  }
}
