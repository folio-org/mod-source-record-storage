package org.folio.dao.util.executor;

import io.vertx.core.Context;
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
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import java.nio.channels.ClosedChannelException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A query executor that uses {@link Pool} connection pool to execute queries.
 */
public class PgPoolQueryExecutor implements QueryExecutor {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final DSLContext dslContext = DSL.using(SQLDialect.POSTGRES, new Settings()
    .withParamType(ParamType.NAMED)
    .withRenderNamedParamPrefix("$"));

  private final Pool pool;
  private final int numRetries;
  private long retryDelay = 1000;

  public PgPoolQueryExecutor(Pool pool, int numRetries, long retryDelay) {
    this.pool = pool;
    this.numRetries = numRetries;
    this.retryDelay = retryDelay;
  }

  @Override
  public Future<RowSet<Row>> execute(Function<DSLContext, Query> queryFunction) {
    Query query = queryFunction.apply(dslContext);
    return pool.preparedQuery(query.getSQL())
      .execute(Tuple.from(query.getBindValues()));
  }

  /**
   * Executes the specified {@code action} within a transaction.
   *
   * @param action the action to execute within the transaction
   * @param <R>    the type of the result of the action
   * @return {@link Future} with the result of the transaction execution
   */
  public <R> Future<R> transaction(Function<QueryExecutor, Future<R>> action) {
    return retryOf(
      () -> pool.withTransaction((SqlConnection connection) -> action.apply(new TransactionBoundQueryExecutor(connection))),
      numRetries);
  }

  /**
   * Helper method for retrying operations. Introduces
   * a delay and recursively retries in case of specific exceptions.
   *
   * @param supplier the operation to retry
   * @param times    the number of times to retry
   * @return the result of the operation
   */
  private <U> Future<U> retryOf(Supplier<Future<U>> supplier, int times) {
    if (times <= 0) {
      return supplier.get();
    }

    return supplier.get().recover(err -> {
      if (err instanceof NoStackTraceThrowable || err instanceof ClosedChannelException
        || err instanceof ClosedConnectionException) {

        Promise<U> promise = Promise.promise();
        Context currentContext = Vertx.currentContext();
        if (currentContext == null) { // don't introduce a delay
          LOGGER.error("Execution error during proxied call. Retrying...", err);
          return retryOf(supplier, times - 1);
        } else {
          Vertx vertx = currentContext.owner();
          LOGGER.error("Execution error during proxied call. Retry in {}ms...", retryDelay, err);
          vertx.setTimer(retryDelay, timerId ->  // Introduce a delay
            retryOf(supplier, times - 1).onComplete(promise) // Recursively call retryOf and pass on the result
          );
          return promise.future();
        }
      }

      return Future.failedFuture(err);
    });
  }

  /**
   * A query executor that is bound to a specific transaction.
   */
  private class TransactionBoundQueryExecutor implements QueryExecutor {

    private final SqlConnection connection;

    private TransactionBoundQueryExecutor(SqlConnection connection) {
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
