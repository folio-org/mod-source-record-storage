package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.reactivex.sqlclient.Pool;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Tuple;
import io.vertx.sqlclient.ClosedConnectionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import java.nio.channels.ClosedChannelException;
import java.util.function.Function;
import java.util.function.Supplier;

public class QueryExecutor implements CommonQueryExecutor {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final DSLContext dslContext = DSL.using(SQLDialect.POSTGRES, new Settings()
    .withParamType(ParamType.NAMED)
    .withRenderNamedParamPrefix("$"));

  private final Pool pool;
  private final int numRetries;
  private long retryDelay = 1000;

  public QueryExecutor(Pool pool, int numRetries, long retryDelay) {
    this.pool = pool;
    this.numRetries = numRetries;
    this.retryDelay = retryDelay;
  }

  public QueryExecutor(Pool pool, Integer numOfRetries) {
    this.pool = pool;
    this.numRetries = numOfRetries;
  }

//  public Future<RowSet<Row>> execute(Function<DSLContext, ResultQuery<?>> action) {
  public Future<RowSet<Row>> execute(Function<DSLContext, Query> queryFunction) {
    Query query = queryFunction.apply(dslContext);
    return pool.preparedQuery(query.getSQL())
      .execute(Tuple.from(query.getBindValues()));
  }

  public <T> Future<T> transaction(Function<TransactionQueryExecutor, Future<T>> action) {
    return retry(
      () -> pool.withTransaction((SqlConnection connection) -> action.apply(new TransactionQueryExecutor(connection))),
      numRetries);
  }

  private <T> Future<T> retry(Supplier<Future<T>> supplier, int times) {
    if (times <= 0) {
      return supplier.get();
    }

    return supplier.get().recover(err -> {
      if (err instanceof NoStackTraceThrowable || err instanceof ClosedChannelException || err instanceof ClosedConnectionException) {
        Promise<T> promise = Promise.promise();
        LOGGER.error("Execution error during database query. Retrying in {}ms...", retryDelay, err);
        Vertx.currentContext().owner().setTimer(retryDelay, timerId ->
          retry(supplier, times - 1).onComplete(promise)
        );
        return promise.future();
      }
      return Future.failedFuture(err);
    });
  }

  public class TransactionQueryExecutor implements CommonQueryExecutor {

    private final SqlConnection connection;

    private TransactionQueryExecutor(SqlConnection connection) {
      this.connection = connection;
    }

    @Override
    public Future<RowSet<Row>> execute(Function<DSLContext, Query> queryFunction) {
      Query query = queryFunction.apply(dslContext);
      return connection.preparedQuery(query.getSQL())
        .execute(Tuple.from(query.getBindValues()));
    }

  }
}
