package org.folio.dao.util.executor;

import io.vertx.core.Future;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import org.jooq.DSLContext;
import org.jooq.Query;

import java.util.function.Function;

/**
 * An asynchronous query executor.
 */
public interface QueryExecutor {

  /**
   * Executes a query provided by {@code queryFunction} and returns the result of the execution.
   * The provided {@code queryFunction} receives a {@link DSLContext} to construct the query.
   * The resulting {@link Query} object is then executed asynchronously.
   *
   * @param queryFunction - a function that returns query for execution
   * @return {@link Future} that will be completed with the {@link RowSet} containing the query results,
   *         or failed if the query execution fails.
   */
  Future<RowSet<Row>> execute(Function<DSLContext, Query> queryFunction);

}
