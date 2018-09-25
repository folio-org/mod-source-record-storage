package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.folio.rest.jaxrs.resource.DataStorageResource;

import javax.ws.rs.core.Response;
import java.util.Map;

public class DataStorageImpl implements DataStorageResource {

  private static final String ITEM_STUB_PATH = "ramls/examples/item.sample";
  private static final String HEADER_CONTENT_TYPE = "Content-Type";
  private static final String APPLICATION_JSON = "application/json";

  @Override
  public void getDataStorageItemsByItemId(final String itemId,
                                          final Map<String, String> okapiHeaders,
                                          final Handler<AsyncResult<Response>> asyncResultHandler,
                                          final Context vertxContext) throws Exception {
    //TODO replace stub response
    vertxContext.owner().fileSystem().readFile(ITEM_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(
          Future.succeededFuture(
            Response.ok(event.result().toString()).header(HEADER_CONTENT_TYPE, APPLICATION_JSON).build()
          ));
      } else {
        asyncResultHandler.handle(Future.succeededFuture(
          GetDataStorageItemsByItemIdResponse.withPlainNotFound("Item not found")
        ));
      }
    });
  }

  @Override
  public void deleteDataStorageItemsByItemId(final String itemId,
                                             final Map<String, String> okapiHeaders,
                                             final Handler<AsyncResult<Response>> asyncResultHandler,
                                             final Context vertxContext) throws Exception {
    //TODO replace stub response
    asyncResultHandler.handle(Future.succeededFuture(Response.noContent().build()));
  }
}
