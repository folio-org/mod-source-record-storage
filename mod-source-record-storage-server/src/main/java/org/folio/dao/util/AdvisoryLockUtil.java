package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Tuple;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import static org.jooq.impl.DSL.val;

public class AdvisoryLockUtil {

  private static final String GET_LOCK_FUNCTION = "pg_try_advisory_xact_lock";

  private AdvisoryLockUtil() {
  }

  /**
   * Obtains a transactional level lock in the database by specified {@code key1} and {@code key2} keys
   * using the passed transaction {@code sqlConnection}.
   *
   * @param sqlConnection connection
   * @param key1          first key for lock identification
   * @param key2          second key for lock identification
   * @return future with {@code true} if lock successfully obtained, otherwise {@code false}
   * if the lock by specified keys is already obtained by another transaction.
   */
  public static Future<Boolean> acquireLock(SqlConnection sqlConnection, int key1, int key2) {
    return sqlConnection.preparedQuery(dslContext.select(DSL.field("{0}", Boolean.class,
        DSL.function(GET_LOCK_FUNCTION, SQLDataType.BOOLEAN, val(key1), val(key2)))).getSQL())
      .execute(Tuple.of(key1, key2))
      .map(rowSet -> rowSet.iterator().next().getBoolean(0));
  }

  private static final DSLContext dslContext = DSL.using(SQLDialect.POSTGRES, new Settings()
    .withParamType(ParamType.NAMED)
    .withRenderNamedParamPrefix("$"));

}
