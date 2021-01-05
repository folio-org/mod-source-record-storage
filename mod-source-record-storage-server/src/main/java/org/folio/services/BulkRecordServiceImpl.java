package org.folio.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.RoutingContext;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import org.folio.dao.BulkRecordDao;
import org.folio.rest.util.OkapiConnectionParams;
import org.jooq.Condition;
import org.jooq.Cursor;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.core.Response;

@Service
public class BulkRecordServiceImpl implements BulkRecordService {

  private BulkRecordDao bulkRecordDao;

  public BulkRecordServiceImpl(@Autowired BulkRecordDao bulkRecordDao) {
    this.bulkRecordDao = bulkRecordDao;
  }

  @Override
  public Future<Cursor> searchRecords(OkapiConnectionParams params) {
    Condition noCondition = DSL.noCondition();
    return bulkRecordDao.searchRecords(noCondition, params.getTenantId());
  }

  public void dummySearchRecords(RoutingContext routingContext, Handler<AsyncResult<Response>> asyncResultHandler) {
    String query = generateSQLQuery();
    Tuple params = getParamsForQuery();

    Future<RowStream<Row>> rowStreamFuture = bulkRecordDao.dummySearchRecords(query, params);

    rowStreamFuture.onComplete(ar -> {
      RowStream<Row> rowStream = ar.result();
      rowStream.pipeTo(new WriteStreamRowToResponseWrapper(routingContext.response()) {}, completionEvent -> {
        rowStream.close(arc -> {
          asyncResultHandler.handle(Future.succeededFuture());//???
        });
      });
    });
  }

  private static class WriteStreamRowToResponseWrapper implements WriteStream<Row> {
    private final HttpServerResponse delegate;


    private WriteStreamRowToResponseWrapper(HttpServerResponse delegate) {
      this.delegate = delegate;
    }
  }
}
