package org.folio.dao.util;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import static org.jooq.impl.DSL.val;

public class AdvisoryLockUtil {

  private static final String GET_LOCK_FUNCTION = "pg_try_advisory_xact_lock";

  private static final Logger LOG = LogManager.getLogger();

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
  public static Future<Boolean> acquireLock(ReactiveClassicGenericQueryExecutor queryExecutor, int key1, int key2) {
    return queryExecutor.findOneRow(dsl -> dsl.select(DSL.field("{0}", Boolean.class,
      DSL.function(GET_LOCK_FUNCTION, SQLDataType.BOOLEAN, val(key1), val(key2))))
    ).map(row -> row.getBoolean(0));
  }

}
