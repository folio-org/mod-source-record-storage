package org.folio.dao.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLConnection;
import org.folio.rest.persist.PostgresClient;

import java.util.function.Function;

/**
 * Util class containing helper methods for interacting with db
 */
public final class DbUtil {

  private DbUtil(){}

  private static final Logger LOG = LoggerFactory.getLogger(DbUtil.class);

  /**
   * Executes passed action in transaction
   *
   * @param postgresClient Postgres Client
   * @param action action that needs to be executed in transaction
   * @param <T> result type returned from the action
   * @return future with action result if succeeded or failed future
   */
  public static <T> Future<T> executeInTransaction(PostgresClient postgresClient,
                                                   Function<AsyncResult<SQLConnection>, Future<T>> action) {
    Future<T> future = Future.future();
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        postgresClient.startTx(tx.completer());
        return tx;
      })
      .compose(v -> action.apply(tx))
      .setHandler(result -> {
        if (result.succeeded()) {
          postgresClient.endTx(tx, endTx -> future.complete(result.result()));
        } else {
          postgresClient.rollbackTx(tx, r -> {
            LOG.error("Rollback transaction", result.cause());
            future.fail(result.cause());
          });
        }
      });
    return future;
  }
}
