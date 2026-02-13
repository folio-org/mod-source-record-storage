package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import org.jooq.DSLContext;
import org.jooq.Query;

import java.util.function.Function;

public interface CommonQueryExecutor {

  Future<RowSet<Row>> execute(Function<DSLContext, Query> queryFunction);

}

