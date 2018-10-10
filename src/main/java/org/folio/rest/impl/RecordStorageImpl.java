package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import org.folio.rest.jaxrs.resource.RecordStorage;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

public class RecordStorageImpl implements RecordStorage {

  private static final String ITEM_STUB_PATH = "ramls/examples/item.sample";
  private static final String ITEMS_STUB_PATH = "ramls/examples/items.sample";
  private static final String LOGS_STUB_PATH = "ramls/examples/logs.sample";

  @Override
  public void getRecordStorageItems(String sortBy, int offset, int limit, String query, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    //TODO replace stub response
    vertxContext.owner().fileSystem().readFile(ITEMS_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(
          Future.succeededFuture(
            Response.ok(event.result().toString())
              .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
              .build()
          ));
      } else {
        asyncResultHandler.handle(Future.succeededFuture(
          GetRecordStorageItemsResponse.respond500WithTextPlain("Internal server error: query=" + query)
        ));
      }
    });
  }

  @Override
  public void getRecordStorageItemsByItemId(final String itemId,
                                            final Map<String, String> okapiHeaders,
                                            final Handler<AsyncResult<Response>> asyncResultHandler,
                                            final Context vertxContext) {
    //TODO replace stub response
    vertxContext.owner().fileSystem().readFile(ITEM_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(
          Future.succeededFuture(
            Response.ok(event.result().toString())
              .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
              .build()
          ));
      } else {
        asyncResultHandler.handle(Future.succeededFuture(
          GetRecordStorageItemsByItemIdResponse.respond500WithTextPlain("Internal server error: itemId=" + itemId)
        ));
      }
    });
  }

  @Override
  public void deleteRecordStorageItemsByItemId(final String itemId,
                                               final Map<String, String> okapiHeaders,
                                               final Handler<AsyncResult<Response>> asyncResultHandler,
                                               final Context vertxContext) {
    //TODO replace stub response
    asyncResultHandler.handle(Future.succeededFuture(DeleteRecordStorageItemsByItemIdResponse.respond204WithTextPlain(itemId)));
  }

  @Override
  public void getRecordStorageLogs(String sortBy, int offset, int limit, String query, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    //TODO replace stub response
    vertxContext.owner().fileSystem().readFile(LOGS_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(
          Future.succeededFuture(
            Response.ok(event.result().toString())
              .header(HttpHeaders.CONTENT_TYPE.toString(), MediaType.APPLICATION_JSON)
              .build()
          ));
      } else {
        asyncResultHandler.handle(Future.succeededFuture(
          GetRecordStorageLogsResponse.respond500WithTextPlain("Internal server error: query=" + query)
        ));
      }
    });
  }
}
