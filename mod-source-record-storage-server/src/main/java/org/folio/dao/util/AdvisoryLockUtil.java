package org.folio.dao.util;

import io.vertx.core.Future;
import org.folio.dao.util.executor.QueryExecutor;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import static org.jooq.impl.DSL.val;

public class AdvisoryLockUtil {

  private static final String GET_LOCK_FUNCTION = "pg_try_advisory_xact_lock";

  private AdvisoryLockUtil() {
  }

  /**
   * Obtains a transactional level lock in the database by specified {@code key1} and {@code key2} keys
   * using the passed transactional {@code queryExecutor}.
   *
   * @param queryExecutor query executor
   * @param key1          first key for lock identification
   * @param key2          second key for lock identification
   * @return future with {@code true} if lock successfully obtained, otherwise {@code false}
   * if the lock by specified keys is already obtained by another transaction.
   */
  public static Future<Boolean> acquireLock(QueryExecutor queryExecutor, int key1, int key2) {
    return queryExecutor.execute(dsl -> dsl.select(DSL.field("{0}", Boolean.class,
      DSL.function(GET_LOCK_FUNCTION, SQLDataType.BOOLEAN, val(key1), val(key2))))
    ).map(rowSet -> rowSet.iterator().next().getBoolean(0));
  }

}
