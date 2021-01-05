package org.folio.services;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.folio.rest.util.OkapiConnectionParams;
import org.jooq.Cursor;

import javax.ws.rs.core.Response;

public interface BulkRecordService {

  Future<Cursor> searchRecords(OkapiConnectionParams params);

  void dummySearchRecords(RoutingContext routingContext, Handler<AsyncResult<Response>> asyncResultHandler);

}
