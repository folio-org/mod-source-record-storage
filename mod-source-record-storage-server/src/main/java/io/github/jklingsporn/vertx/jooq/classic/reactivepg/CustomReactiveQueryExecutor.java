package io.github.jklingsporn.vertx.jooq.classic.reactivepg;


import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Transaction;
import org.jooq.Configuration;
import org.jooq.Query;
import org.jooq.SQLDialect;
import org.jooq.conf.ParamType;

import java.util.function.Function;

/**
 * This class was moved to this package so that we can access the  constructor that accepts a transaction. The constructor
 * is only accessible from this package.
 */
public class CustomReactiveQueryExecutor extends ReactiveClassicGenericQueryExecutor {
  private final String tenantId;
  private static final String pattern = "(?<![:=]):(?![:=])";

  public CustomReactiveQueryExecutor(Configuration configuration, SqlClient delegate, String tenantId) {
    this(configuration, delegate, null, tenantId);
  }

  CustomReactiveQueryExecutor(Configuration configuration, SqlClient delegate, Transaction transaction, String tenantId) {
    super(configuration, delegate, transaction);
    this.tenantId = tenantId;
  }

  /**
   * This helps to retain the tenant context for the query executor
   */
  public String getTenantId() {
    return tenantId;
  }

  @Override
  protected Function<Transaction, ? extends CustomReactiveQueryExecutor> newInstance(SqlClient connection) {
    return transaction -> new CustomReactiveQueryExecutor(configuration(), connection, transaction, tenantId);
  }

  @SuppressWarnings("unchecked")
  public <U> Future<U> customTransaction(Function<? super CustomReactiveQueryExecutor, Future<U>> transaction){
    return this.transaction((Function<ReactiveClassicGenericQueryExecutor, Future<U>>) transaction);
  }

  /**
   * This method was copied from the super class. The only difference is the pattern used to replace characters
   * in the named query. The pattern in the super class did not handle some cases.
   */
  @Override
  public String toPreparedQuery(Query query) {
    if (SQLDialect.POSTGRES.supports(configuration().dialect())) {
      String namedQuery = query.getSQL(ParamType.NAMED);
      return namedQuery.replaceAll(pattern, "\\$");
    }
    // mysql works with the standard string
    return query.getSQL();
  }



  /**
   * This is a hack to expose the underlying vertx sql client because vertx-jooq does not support batch operations.
   */
  public Future<RowSet<Row>> getDelegate(Function<SqlClient, Future<RowSet<Row>>> delegateFunction){
    return delegateFunction.apply(delegate);
  }
}
